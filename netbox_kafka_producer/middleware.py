import collections
import confluent_kafka
import dictdiffer
import re
import socket
import time
import uuid

from django.conf             import settings
from django.core.serializers import json
from django.db.models        import signals
from django.utils.functional import curry

from utilities.api import get_serializer_for_model

def signal_post_delete(record, sender, instance, **kwargs):
	record('delete', sender, instance)

def signal_post_save(record, sender, instance, created, **kwargs):
	events = {True: 'create', False: 'update'}

	record(events[created], sender, instance)

def signal_pre_delete(record, sender, instance, **kwargs):
	record('delete', sender, instance)

def signal_pre_save(record, sender, instance, **kwargs):
	events = {True: 'update', False: 'create'}

	record(events[bool(instance.pk)], sender, instance)

# Record is a simple container class for bundling signal information. The model
# is the serialized (dict) version of the instance, generated when required.
class Event:
	def __init__(self, event, sender, instance, model=None):
		self.event    = event
		self.sender   = sender
		self.instance = instance
		self.model    = model

class Transaction:
	def __init__(self, ignore, request):
		self.ignore  = ignore
		self.request = request
		self.streams = collections.defaultdict(list)

	# Event is create, update, or delete, as determined by the signal handler.
	def record(self, event, sender, instance):
		# Ignore sender?
		sender_class = sender.__module__ + '.' + sender.__qualname__

		for pattern in self.ignore:
			if pattern.match(sender_class):
				return

		# Ignore unserializable models.
		try:
			get_serializer_for_model(instance)
		except:
			return

		model  = None
		stream = self.streams[id(instance)]

		# The first instance must be serialized before related records are
		# updated.
		if len(stream) == 0:
			model = self.serialize(sender, instance)

		# Store only the first and last events for each instance.
		if len(stream) == 2:
			stream.pop()

		stream.append(Event(event, sender, instance, model))

	def serialize(self, sender, instance, prefix=''):
		# A new record that has not been written to the database.
		if not instance.pk:
			return None

		# The request is required for URLs to be rendered correctly.
		context  = {'request': self.request}
		instance = sender.objects.get(pk=instance.pk)

		serializer = get_serializer_for_model(instance, prefix)
		serialized = serializer(instance, context=context)

		model = serialized.data

		# Prevent dictdiffer from trying to recurse infinitely.
		if 'tags' in model:
			model['tags'] = list(model['tags'])

		return model

# Tracking changes is accomplished by observing signals emitted for models
# created, updated, or deleted during a request. In order to determine the
# difference between two models, the first and last instance are stored in
# the per-instance "stream", partitioned by id(instance).
class KafkaChangeMiddleware:
	def __init__(self, get_response):
		self.get_response = get_response

		self.encoder = json.DjangoJSONEncoder()
		self.servers = settings.KAFKA['SERVERS']
		self.topic   = settings.KAFKA['TOPIC']

		self.producer = confluent_kafka.Producer({
			'bootstrap.servers':       self.servers,
			'socket.keepalive.enable': True,
		})

		# Ignore senders that provide duplicate or uninteresting information.
		self.ignore = list(map(lambda pattern: re.compile(pattern), [
			'django.contrib',
			'extras.models.CustomFieldValue',
			'extras.models.ObjectChange',
			'taggit',
			'users.models.Token',
		]))

	def __call__(self, request):
		tx = Transaction(self.ignore, request)
		cb = lambda fn: curry(fn, tx.record)

		connections = [
			( signals.post_delete, cb(signal_post_delete) ),
			( signals.post_save,   cb(signal_post_save)   ),
			( signals.pre_delete,  cb(signal_pre_delete)  ),
			( signals.pre_save,    cb(signal_pre_save)    ),
		]

		for signal, receiver in connections:
			signal.connect(receiver)

		messages = []
		response = self.get_response(request)

		for signal, receiver in connections:
			signal.disconnect(receiver)

		# streams = {
		#   id(instance): [Record, Record, ...]
		# }
		for stream in tx.streams.values():
			# Ignore failed requests, where a 'post_*' signal was not received.
			if len(stream) != 2:
				continue

			head, tail = stream

			data = {
				'create': tail,
				'update': tail,
				'delete': head, # Publish the model prior to deletion.
			}[head.event]

			# Loading and serializing the last instance is deferred to as late
			# as possible, in order to capture all related changes, such as tag
			# and custom field updates.
			if data.model is None:
				data.model = tx.serialize(data.sender, data.instance)

			message = collections.defaultdict(dict, {
				'class': data.sender.__name__,
				'event': data.event,
				'model': data.model,
			})

			messages.append(message)

			# In order for the consumer to easily build a pynetbox record,
			# include the absolute URL.
			if data.event != 'delete':
				try:
					nested = tx.serialize(data.sender, data.instance, 'Nested')

					if nested and 'url' in nested:
						message['@url'] = nested['url']
				except:
					pass

			# No need to find differences unless an existing model was updated.
			if data.event != 'update':
				continue

			for change in dictdiffer.diff(head.model, tail.model, expand=True):
				field = change[1]

				# Array change.
				if isinstance(field, list):
					field = field[0]

				message['detail'].update({
					field: [
						dictdiffer.dot_lookup(head.model, field),
						dictdiffer.dot_lookup(tail.model, field),
					]
				})

		if messages:
			self.publish(messages, self.common(request))

		return response

	# Common metadata from the request, to be included with each message.
	def common(self, request):
		addr = request.META['REMOTE_ADDR']
		user = request.user.get_username()

		# Handle being behind a proxy.
		if 'HTTP_X_FORWARDED_FOR' in request.META:
			addr = request.META['HTTP_X_FORWARDED_FOR']

		# RFC3339 timestamp.
		timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

		return {
			'@timestamp': timestamp,
			'request': {
				'addr': addr,
				'user': user,
				'uuid': uuid.uuid4().hex,
			},
			'response': {
				'host': socket.gethostname(),
			},
		}

	def publish(self, messages, common):
		for message in messages:
			message.update(common)

			self.producer.produce(self.topic, self.encoder.encode(message))

		self.producer.flush()
