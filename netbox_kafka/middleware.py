import collections
import confluent_kafka
import dictdiffer
import json
import logging
import threading

from django.conf      import settings
from django.db.models import signals

from django.core.serializers.json import DjangoJSONEncoder

from utilities.api import get_serializer_for_model

_thread_locals = threading.local()

class KafkaChangeMiddleware:
	def __init__(self, get_response):
		self.get_response = get_response

		self.log = logging.getLogger(__name__)

		connections = {
			signals.m2m_changed: self.signal_m2m_changed,
			signals.post_save:   self.signal_post_save,	
			signals.pre_delete:  self.signal_pre_delete,	
			signals.pre_save:    self.signal_pre_save,	
		}

		for signal, receiver in connections.items():
			signal.connect(receiver, dispatch_uid=__name__)

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
				'class': head['class'],
				'event': head['event'],
				'model': tail['model'],
			})

			if not head['event']:
				message['event'] = tail['event']

			A = head['model']
			B = tail['model']

			for change in dictdiffer.diff(A, B, expand=True, ignore=set(['tags'])):
				field = change[1]

				# Array change.
				if isinstance(field, list):
					field = field[0]

				message['detail'].update({
					field: [
						dictdiffer.dot_lookup(A, field),
						dictdiffer.dot_lookup(B, field),
					]
				})

			messages.append(message)

		if messages:
			self.publish(messages)

		return response

	def publish(self, messages):
		for message in messages:
			payload = json.dumps(message, cls=DjangoJSONEncoder)

			self.producer.produce(self.topic, payload)

		self.producer.flush()

	# events = {
	# 	'class:pk': [...],
	# }
	def store(self, event, model):
		try:
			serializer = get_serializer_for_model(model)
		except:
			return

		serialized = serializer(model, context={'request': None})

		stream = model._meta.label + ':' + str(model.id)

		_thread_locals.events[stream].append({
			'class': type(model).__name__,
			'event': event,
			'model': serialized.data,
		})

	def signal_m2m_changed(self, sender, instance, **kwargs):
		self.store('update', instance)

	def signal_post_save(self, sender, instance, created, **kwargs):
		if created:
			self.store('create', instance)
		else:
			self.store('update', instance)

	def signal_pre_delete(self, sender, instance, **kwargs):
		self.store('delete', instance)

	def signal_pre_save(self, sender, instance, **kwargs):
		instance = sender.objects.filter(pk=instance.id).first()

		if instance:
			self.store(None, instance)
