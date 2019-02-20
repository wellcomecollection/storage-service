#!/usr/bin/env bash

# Release tooling
alias release="$(PWD)/docker_run.py --root --aws -- -it wellcome/release_tooling:65"

# Terraform
alias tf="$(PWD)/docker_run.py --root --aws -- -it --workdir=$(PWD)/terraform hashicorp/terraform:0.11.11"