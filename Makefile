default: clean dist

clean:
	rm -rf build dist netbox_kafka_producer.egg-info

dist:
	python3 setup.py sdist bdist_wheel
	python3 -m twine check  dist/*
	python3 -m twine upload dist/*
