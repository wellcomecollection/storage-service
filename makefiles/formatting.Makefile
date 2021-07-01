ROOT = $(shell git rev-parse --show-toplevel)

ECR_REGISTRY = 760097843905.dkr.ecr.eu-west-1.amazonaws.com

lint-python:
	docker run --tty --rm \
		--volume $(ROOT):/data \
		--workdir /data \
		$(ECR_REGISTRY)/wellcome/flake8:latest \
		    --exclude .git,__pycache__,target,.terraform \
		    --ignore=E501,E122,E126,E203,W503

format-terraform:
	docker run --tty --rm \
		--volume $(ROOT):/repo \
		--workdir /repo \
		$(ECR_REGISTRY)/hashicorp/terraform:light fmt -recursive

format-python:
	docker run --tty --rm \
		--volume $(ROOT):/repo \
		$(ECR_REGISTRY)/wellcome/format_python:112

format: format-terraform format-python
	$(ROOT)/makefiles/run_scalafmt.sh

lint: lint-python
	git diff --exit-code
