import collections
import confluent_kafka
import dictdiffer
import logging
import socket
import threading
import time
import uuid

from django.conf             import settings
from django.core.serializers import json
from django.db.models        import signals

from utilities.api import get_serializer_for_model

_thread_locals = threading.local()

class Record:
	def __init__(self, event, sender, instance):
		self.event    = event
		self.sender   = sender
		self.instance = instance

		self.model = None

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
		self.logger  = logging.getLogger(__name__)

		self.servers = settings.KAFKA['SERVERS']
		self.topic   = settings.KAFKA['TOPIC']

		self.producer = confluent_kafka.Producer({
			'bootstrap.servers':       self.servers,
			'socket.keepalive.enable': True,
		})

		# Ignore signal senders that provide duplicate information.
		self.ignore = ('CustomFieldValue', 'ObjectChange', 'TaggedItem')

	def __call__(self, request):
		_thread_locals.request = request
		_thread_locals.streams = collections.defaultdict(list)

		messages = []
		response = self.get_response(request)

		for stream in _thread_locals.streams.values():
			head = stream[0]
			tail = stream[-1]

			if tail.model is None:
				tail.model = self.serialize(tail.sender, tail.instance)

			message = collections.defaultdict(dict)

			message.update({
				'class': head.sender.__name__,
				'event': head.event,
				'model': tail.model,
			})

			messages.append(message)

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

	def common(self, request):
		addr = request.META['REMOTE_ADDR']
		user = request.user.get_username()

		# Handle being behind a proxy.
		if 'HTTP_X_FORWARDED_FOR' in request.META:
			addr = request.META['HTTP_X_FORWARDED_FOR'][0]

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

	def record(self, event, sender, instance):
		if sender.__name__ in self.ignore:
			return

		record = Record(event, sender, instance)
		stream = _thread_locals.streams[id(instance)]

		# The first record must be retrieved before related records are
		# possibly updated.
		if len(stream) == 0:
			record.model = self.serialize(sender, instance)

		# Store only the first and last records for each instance.
		if len(stream) == 2:
			stream.pop()

		stream.append(record)

	def serialize(self, sender, instance):
		# A new record that has not been written to the database.
		if not instance.pk:
			return {}

		try:
			serializer = get_serializer_for_model(instance)
		except Exception as err:
			self.logger.warn('get_serializer_for_model: {}'.format(err))

			return {}

		# The request is required for the URLs to be rendered correctly.
		context = {'request': _thread_locals.request}

		model = serializer(sender.objects.get(pk=instance.pk), context=context)
		model = model.data

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
