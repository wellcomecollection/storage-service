ROOT = $(shell git rev-parse --show-toplevel)

ifneq ($(TRAVIS),true)
DEV_ROLE_ARN := arn:aws:iam::975596993436:role/storage-developer
endif


INFRA_BUCKET = wellcomecollection-platform-infra


include $(ROOT)/makefiles/terraform.Makefile


# Publish a ZIP file containing a Lambda definition to S3.
#
# Args:
#   $1 - Path to the Lambda src directory, relative to the root of the repo.
#
define publish_lambda
    $(ROOT)/docker_run.py --aws --root -- \
        wellcome/publish_lambda:14 \
        "$(1)" --key="lambdas/$(1).zip" --bucket="$(INFRA_BUCKET)" --sns-topic="arn:aws:sns:eu-west-1:760097843905:lambda_pushes"
endef


# Test a Python project.
#
# Args:
#   $1 - Path to the Python project's directory, relative to the root
#        of the repo.
#
define test_python
	$(ROOT)/docker_run.py --aws --dind -- \
		wellcome/build_test_python $(1)

	$(ROOT)/docker_run.py --aws --dind -- \
		--net=host \
		--volume $(ROOT)/shared_conftest.py:/conftest.py \
		--workdir $(ROOT)/$(1) --tty \
		wellcome/test_python_$(shell basename $(1)):latest
endef


# Build and tag a Docker image.
#
# Args:
#   $1 - Name of the image.
#   $2 - Path to the Dockerfile, relative to the root of the repo.
#
define build_image
	$(ROOT)/docker_run.py \
	    --dind -- \
	    wellcome/image_builder:23 \
            --project=$(1) \
            --file=$(2)
endef


# Publish a Docker image to ECR, and put its associated release ID in S3.
#
# Args:
#   $1 - Name of the Docker image
#   $2 - Stack name
#   $3 - ECR Repository URI
#   $4 - Registry ID
#
define publish_service
	$(ROOT)/docker_run.py \
	    --aws --dind -- \
	    wellcome/publish_service:86 \
	    	--service_id="$(1)" \
	        --project_id=$(2) \
	        --account_id=$(3) \
	        --region_id=eu-west-1 \
	        --namespace=uk.ac.wellcome \
	        --role_arn="$(DEV_ROLE_ARN)" \
	        --label=latest
endef


# Test an sbt project.
#
# Args:c
#   $1 - Name of the project.
#
define sbt_test
	$(ROOT)/docker_run.py --dind --sbt --root -- \
		--net host \
		wellcome/sbt_wrapper:edge \
		"project $(1)" ";dockerComposeUp;test;dockerComposeStop"
endef

# Test an sbt project without docker-compose.
#
# Args:
#   $1 - Name of the project.
#
define sbt_test_no_docker
	$(ROOT)/docker_run.py --dind --sbt --root -- \
		--net host \
		wellcome/sbt_wrapper:edge \
		"project $(1)" "test"
endef


# Build an sbt project.
#
# Args:
#   $1 - Name of the project.
#
define sbt_build
	$(ROOT)/docker_run.py --sbt --root -- \
		wellcome/sbt_wrapper:edge \
		"project $(1)" ";stage"
endef


# Run docker-compose up.
#
# Args:
#   $1 - Path to the docker-compose file.
#
define docker_compose_up
	$(ROOT)/docker_run.py --dind --sbt --root -- \
		--net host \
		wellcome/sbt_wrapper:edge \
		"project $(1)" "dockerComposeUp"
endef


# Run docker-compose down.
#
# Args:
#   $1 - Path to the docker-compose file.
#
define docker_compose_down
	$(ROOT)/docker_run.py --dind --sbt --root -- \
		--net host \
		wellcome/sbt_wrapper:edge \
		"project $(1)" "dockerComposeDown"
endef


# Define a series of Make tasks (build, test, publish) for a Scala services.
#
# Args:
#	$1 - Name of the project in sbt.
#	$2 - Root of the project's source code.
#	$3 - Stack name
#   $4 - ECS Base URI
#   $5 - Registry ID
#
define __sbt_docker_target_template
$(eval $(call __sbt_base_docker_template,$(1),$(2)))

$(1)-build:
	$(call sbt_build,$(1))
	$(call build_image,$(1),$(2)/Dockerfile)

$(1)-publish: $(1)-build
	$(call publish_service,$(1),$(3),$(4),$(5))
endef



define __sbt_no_docker_target_template
$(1)-test:
	$(call sbt_test_no_docker,$(1))

$(1)-build:
	$(call sbt_build,$(1))
	$(call build_image,$(1),$(2)/Dockerfile)

$(1)-publish: $(1)-build
	$(call publish_service,$(1),$(3),$(4),$(5))
endef


# Define a series of Make tasks for a Scala libraries that use docker-compose for tests.
#
# Args:
#	$1 - Name of the project in sbt.
#	$2 - Root of the project's source code.
#
define __sbt_library_docker_template
$(eval $(call __sbt_base_docker_template,$(1),$(2)))

$(1)-publish:
	echo "Nothing to do!"

endef


# Define a series of Make tasks for a Scala modules that use docker-compose for tests.
#
# Args:
#	$1 - Name of the project in sbt.
#	$2 - Root of the project's source code.
#
define __sbt_base_docker_template
$(1)-docker_compose_up:
	$(call docker_compose_up,$(1))

$(1)-docker_compose_down:
	$(call docker_compose_down,$(1))

$(1)-test:
	$(call sbt_test,$(1))

endef


# Define a series of Make tasks for a Scala libraries.
#
# Args:
#	$1 - Name of the project in sbt.
#	$2 - Root of the project's source code.
#
define __sbt_library_template
$(1)-test:
	$(call sbt_test_no_docker,$(1))

$(1)-publish:
	echo "Nothing to do!"

endef


# Define a series of Make tasks (test, publish) for a Python Lambda.
#
# Args:
#	$1 - Name of the target.
#	$2 - Path to the Lambda source directory.
#
define __lambda_target_template
$(1)-test:
	$(call test_python,$(2))

$(1)-publish:
	$(call publish_lambda,$(2))

$(ROOT)/$(2)/src/requirements.txt: $(ROOT)/$(2)/src/requirements.in
	$(ROOT)/docker_run.py -- \
		--volume $(ROOT)/$(2)/src:/src micktwomey/pip-tools

$(ROOT)/$(2)/src/test_requirements.txt: $(ROOT)/$(2)/src/test_requirements.in
	$(ROOT)/docker_run.py -- \
		--volume $(ROOT)/$(2)/src:/src micktwomey/pip-tools \
		pip-compile test_requirements.in
endef


# Define a series of Make tasks (build, test, publish) for an ECS service.
#
# Args:
#	$1 - Name of the ECS service.
#	$2 - Path to the associated Dockerfile.
#	$3 - Stack name
#   $4 - ECS Base URI
#   $5 - Registry ID
#
define __python_ssm_target
$(1)-build:
	$(call build_image,$(1),$(2))

$(1)-test:
	$(call test_python,$(STACK_ROOT)/$(1))

$(1)-publish: $(1)-build
	$(call publish_service,$(1),$(3),$(4),$(5))
endef


# Define all the Make tasks for a stack.
#
# Args:
#
#	$STACK_ROOT             Path to this stack, relative to the repo root
#
#	$SBT_DOCKER_LIBRARIES   A space delimited list of sbt libraries  in this stack that use docker compose for tests
#	$SBT_NO_DOCKER_LIBRARIES   A space delimited list of sbt libraries  in this stack that use docker compose for tests
#	$PYTHON_APPS              A space delimited list of ECS services
#	$LAMBDAS                A space delimited list of Lambdas in this stack
#
#	$TF_NAME                Name of the associated Terraform stack
#	$TF_PATH                Path to the associated Terraform stack
#
define stack_setup

# The structure of each of these lines is as follows:
#
#	$(foreach name,$(NAMES),
#		$(eval
#			$(call __target_template,$(arg1),...,$(argN))
#		)
#	)
#
# It can't actually be written that way because Make is very sensitive to
# whitespace, but that's the general idea.

$(foreach proj,$(SBT_APPS),$(eval $(call __sbt_docker_target_template,$(proj),$(STACK_ROOT)/$(proj),$(PROJECT_ID),$(ACCOUNT_ID))))
$(foreach proj,$(SBT_NO_DOCKER_APPS),$(eval $(call __sbt_no_docker_target_template,$(proj),$(STACK_ROOT)/$(proj),$(PROJECT_ID),$(ACCOUNT_ID))))
$(foreach library,$(SBT_DOCKER_LIBRARIES),$(eval $(call __sbt_library_docker_template,$(library),$(STACK_ROOT)/$(library))))
$(foreach library,$(SBT_NO_DOCKER_LIBRARIES),$(eval $(call __sbt_library_template,$(library))))
$(foreach task,$(PYTHON_APPS),$(eval $(call __python_ssm_target,$(task),$(STACK_ROOT)/$(task)/Dockerfile,$(PROJECT_ID),$(ACCOUNT_ID))))
$(foreach lamb,$(LAMBDAS),$(eval $(call __lambda_target_template,$(lamb),$(STACK_ROOT)/$(lamb))))
$(foreach name,$(TF_NAME),$(eval $(call __terraform_target_template,$(TF_NAME),$(TF_PATH))))
endef