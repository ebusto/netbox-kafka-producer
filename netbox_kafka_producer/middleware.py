import collections
import confluent_kafka
import dictdiffer
import re
import socket
import threading
import time
import uuid

from django.conf             import settings
from django.core.serializers import json
from django.db.models        import signals

from utilities.api import get_serializer_for_model

_thread_locals = threading.local()

# Record is a simple container class for bundling signal information. The model
# is the serialized (dict) version of the instance, generated when required.
class Record:
	def __init__(self, event, sender, instance):
		self.event    = event
		self.sender   = sender
		self.instance = instance

		self.model = None

# Tracking changes is accomplished by observing signals emitted for models
# created, updated, or deleted during a request. In order to determine the
# difference between two models, the first and last instance are stored in
# the per-instance "stream", partitioned by id(instance).
class KafkaChangeMiddleware:
	def __init__(self, get_response):
		self.get_response = get_response

		connections = {
			signals.post_save:  self.signal_post_save,	
			signals.pre_delete: self.signal_pre_delete,	
			signals.pre_save:   self.signal_pre_save,	
		}

		for signal, receiver in connections.items():
			signal.connect(receiver, dispatch_uid=__name__)

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
		_thread_locals.request = request
		_thread_locals.streams = collections.defaultdict(list)

		messages = []
		response = self.get_response(request)

		# streams = {
		#   id(instance): [Record, Record, ...]
		# }
		for stream in _thread_locals.streams.values():
			head = stream[0]
			tail = stream[-1]

			# Loading and serializing the last instance is deferred to as late
			# as possible, in order to capture all related changes, such as tag
			# and custom field updates.
			if tail.model is None:
				tail.model = self.serialize(tail.sender, tail.instance)

			message = collections.defaultdict(dict)

			message.update({
				'class': head.sender.__name__,
				'event': head.event,
				'model': tail.model,
			})

			messages.append(message)

			# In order for the consumer to easily build a pynetbox record,
			# include the absolute URL.
			if head.event != 'delete':
				try:
					nested = self.serialize(tail.sender, tail.instance, 'Nested')

					if nested and 'url' in nested:
						message['@url'] = nested['url']
				except:
					pass

			# No need to find differences unless an existing model was updated.
			if head.event != 'update':
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

	# Event is create, update, or delete, as determined by the signal handler.
	def record(self, event, sender, instance):
		# Determine the fully qualified sender name.
		name = sender.__module__ + '.' + sender.__qualname__

		# Ignore sender?
		if any(map(lambda pattern: pattern.match(name), self.ignore)):
			return

		# Ignore unserializable models.
		try:
			get_serializer_for_model(instance)
		except:
			return

		record = Record(event, sender, instance)
		stream = _thread_locals.streams[id(instance)]

		# The first instance must be serialized before related records are
		# updated.
		if len(stream) == 0:
			record.model = self.serialize(sender, instance)

		# Store only the first and last records for each instance.
		if len(stream) == 2:
			stream.pop()

		stream.append(record)

	def serialize(self, sender, instance, prefix=''):
		# A new record that has not been written to the database.
		if not instance.pk:
			return None

		# The request is required for URLs to be rendered correctly.
		context  = {'request': _thread_locals.request}
		instance = sender.objects.get(pk=instance.pk)

		serializer = get_serializer_for_model(instance, prefix)
		serialized = serializer(instance, context=context)

		model = serialized.data

		# Prevent dictdiffer from trying to recurse infinitely.
		if 'tags' in model:
			model['tags'] = list(model['tags'])

		return model

	def signal_post_save(self, sender, instance, created, **kwargs):
		events = {
			True:  'create',
			False: 'update',
		}

		self.record(events[created], sender, instance)

	def signal_pre_delete(self, sender, instance, **kwargs):
		self.record('delete', sender, instance)

	def signal_pre_save(self, sender, instance, **kwargs):
		events = {
			True:  'update',
			False: 'create',
		}

		self.record(events[bool(instance.pk)], sender, instance)
