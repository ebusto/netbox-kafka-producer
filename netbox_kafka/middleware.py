import collections
import confluent_kafka
import dictdiffer
import logging
import socket
import threading
import uuid

from datetime                import datetime
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
		self._model   = None

	@property
	def model(self):
		if not self._model:
			self.serialize()

		return self._model

	def serialize(self):
		if callable(self.instance):
			self.instance = self.instance()

		try:
			serializer = get_serializer_for_model(self.instance)
		except:
			return None

		self._model = serializer(self.instance, context={'request': None})
		self._model = self._model.data

		# Prevent dictdiffer from trying to recurse infinitely.
		if 'tags' in self._model:
			self._model['tags'] = [str(tag) for tag in self._model['tags']]

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
		self.ignore  = ('CustomFieldValue', 'ObjectChange', 'TaggedItem')
		self.logger  = logging.getLogger(__name__)

		self.servers = settings.KAFKA['SERVERS']
		self.topic   = settings.KAFKA['TOPIC']

		self.producer = confluent_kafka.Producer({
			'bootstrap.servers':       self.servers,
			'socket.keepalive.enable': True,
		})

	def __call__(self, request):
		_thread_locals.events = collections.defaultdict(list)

		messages = []
		response = self.get_response(request)

		for stream in _thread_locals.events.values():
			head = stream[0]
			tail = stream[-1]

			message = collections.defaultdict(dict)

			message.update({
				'class': head.sender.__name__,
				'event': head.event,
				'model': tail.model,
			})

			messages.append(message)

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
			common = {
				'@timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
			}

			# If we're behind a proxy, get the client's IP address.
			if 'HTTP_X_FORWARDED_FOR' in request.META:
				request.META['REMOTE_ADDR'] = request.META['HTTP_X_FORWARDED_FOR']

			# The REMOTE_HOST header is unreliable, so perform a DNS lookup of the IP.
			try:
				request.META['REMOTE_HOST'] = socket.gethostbyaddr(request.META['REMOTE_ADDR'])[0]
			except:
				request.META['REMOTE_HOST'] = None

			common['request'] = {
				'addr': request.META['REMOTE_ADDR'],
				'host': request.META['REMOTE_HOST'],
				'id':   uuid.uuid4().hex,
				'user': request.user.get_username(),
			}

			common['response'] = {
				'host': socket.gethostname(),
			}

			self.publish(messages, common)

		return response

	def publish(self, messages, common):
		for message in messages:
			message.update(common)

			self.producer.produce(self.topic, self.encoder.encode(message))

		self.producer.flush()

	# events = {
	# 	'class:pk': [...],
	# }
	def record(self, event, sender, instance, pk):
		if sender.__name__ in self.ignore:
			return

		record = Record(event, sender, instance)
		stream = str(sender) + str(pk)

		# The first record must be serialized immediately in order to retrieve
		# related records, such as custom fields, before they're modified.
		if len(_thread_locals.events[stream]) == 0 and event != 'create':
			record.serialize()

		# Only store the first and last records for this stream.
		if len(_thread_locals.events[stream]) == 2:
			_thread_locals.events[stream].pop()

		_thread_locals.events[stream].append(record)

	def signal_post_save(self, sender, instance, created, **kwargs):
		event = 'create' if created else 'update'

		self.record(event, sender, lambda: sender.objects.get(pk=instance.id), instance.id)

	def signal_pre_delete(self, sender, instance, **kwargs):
		self.record('delete', sender, instance, instance.id)

	def signal_pre_save(self, sender, instance, **kwargs):
		instance = sender.objects.filter(pk=instance.id).first()

		if instance:
			self.record('update', sender, instance, instance.id)
