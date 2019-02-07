docker_run.py:
	wget https://raw.githubusercontent.com/wellcometrust/docker_run/v1.0/docker_run.py
	chmod u+x docker_run.py

prepare-release: docker_run.py
	./docker_run.py --root --aws -- -it wellcome/release_tooling:54 prepare

deploy-release: docker_run.py
	./docker_run.py --root --aws -- -it wellcome/release_tooling:54 deploy

show-release: docker_run.py
	./docker_run.py --root --aws -- -it wellcome/release_tooling:54 show-release

recent-deployments: docker_run.py
	./docker_run.py --root --aws -- -it wellcome/release_tooling:54 recent-deployments

terraform-init:
	./docker_run.py --root --aws -- -it --workdir=$(PWD)/terraform hashicorp/terraform:0.11.11 init

terraform-apply:
	./docker_run.py --root --aws -- -it --workdir=$(PWD)/terraform hashicorp/terraform:0.11.11 apply