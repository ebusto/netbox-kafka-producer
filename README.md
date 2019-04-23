# Introduction
This module provides middleware to publish [NetBox](https://github.com/digitalocean/netbox/) changes to [Kafka](https://kafka.apache.org/).

# Installation
`pip install netbox-kafka-producer`

# Configuration
Add the following to your NetBox settings.
```
INSTALLED_APPS += (
	'netbox_kafka_producer',
)

MIDDLEWARE += (
	'netbox_kafka_producer.middleware.KafkaChangeMiddleware',
)

KAFKA = {
	'SERVERS': 'kafka01,kafka02,kafka03',
	'TOPIC':   'netbox',
}
```
