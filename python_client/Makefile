ROOT = $(shell git rev-parse --show-toplevel)


python_client-test:
	docker run --tty --volume $(ROOT):$(ROOT) --workdir $(ROOT)/python_client kiwicom/tox
