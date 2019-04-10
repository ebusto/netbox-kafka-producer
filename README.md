# Introduction
This module provides middleware to publish [NetBox](https://github.com/digitalocean/netbox/) changes to [Kafka](https://kafka.apache.org/).

# Configuration
Add the following to your NetBox settings.
```
INSTALLED_APPS += (
	'netbox_kafka',
)

MIDDLEWARE += (
	'netbox_kafka.middleware.KafkaChangeMiddleware',
)

KAFKA = {
	'SERVERS': 'kafka01,kafka02,kafka03',
	'TOPIC':   'netbox',
}
```
