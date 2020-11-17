ROOT = $(shell git rev-parse --show-toplevel)

ECR_REGISTRY = 760097843905.dkr.ecr.eu-west-1.amazonaws.com

lint-python:
	$(ROOT)/docker_run.py -- \
		--volume $(ROOT):/data \
		--workdir /data \
		$(ECR_REGISTRY)/wellcome/flake8:latest \
		    --exclude .git,__pycache__,target,.terraform \
		    --ignore=E501,E122,E126,E203,W503

format-terraform:
	$(ROOT)/docker_run.py --aws -- \
		--volume $(ROOT):/repo \
		--workdir /repo \
		$(ECR_REGISTRY)/hashicorp/terraform:light fmt -recursive

format-python:
	$(ROOT)/docker_run.py -- \
		--volume $(ROOT):/repo \
		$(ECR_REGISTRY)/wellcome/format_python:112

format-scala:
	$(ROOT)/docker_run.py --sbt -- \
		--volume $(ROOT):/repo \
		$(ECR_REGISTRY)/wellcome/scalafmt:edge

format: format-terraform format-scala format-python

lint: lint-python
	git diff --exit-code
