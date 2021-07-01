ROOT = $(shell git rev-parse --show-toplevel)

ECR_REGISTRY = 760097843905.dkr.ecr.eu-west-1.amazonaws.com

INFRA_BUCKET = wellcomecollection-storage-infra


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
            $(ECR_REGISTRY)/wellcome/weco-deploy:5.6.11 \
            --project-id="$(2)" \
            --verbose \
            publish \
            --image-id="$(1)"
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
$(1)-test:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" ";dockerComposeUp;test;dockerComposeStop"

$(1)-build:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" ";stage"
	$(ROOT)/makefiles/build_sbt_image.sh $(1)

$(1)-publish: $(1)-build
	$(call publish_service,$(1),$(3),$(4),$(5))
endef



define __sbt_no_docker_target_template
$(1)-test:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" "test"

$(1)-build:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" ";stage"
	$(ROOT)/makefiles/build_sbt_image.sh $(1)

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
$(1)-test:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" ";dockerComposeUp;test;dockerComposeStop"

$(1)-publish:
	echo "Nothing to do!"

endef


# Define a series of Make tasks for a Scala libraries.
#
# Args:
#	$1 - Name of the project in sbt.
#	$2 - Root of the project's source code.
#
define __sbt_library_template
$(1)-test:
	$(ROOT)/makefiles/run_sbt_task_in_docker.sh "project $(1)" "test"

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
$(1)-publish:
	AWS_PROFILE=storage-dev $(ROOT)/makefiles/publish_lambda_zip.py $(2) \
		--bucket="$(INFRA_BUCKET)" \
		--key="lambdas/$(2).zip"
endef


# Define all the Make tasks for a stack.
#
# Args:
#
#	$STACK_ROOT             Path to this stack, relative to the repo root
#
#	$SBT_DOCKER_LIBRARIES   A space delimited list of sbt libraries  in this stack that use docker compose for tests
#	$SBT_NO_DOCKER_LIBRARIES   A space delimited list of sbt libraries  in this stack that use docker compose for tests
#	$LAMBDAS                A space delimited list of Lambdas in this stack
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
$(foreach lamb,$(LAMBDAS),$(eval $(call __lambda_target_template,$(lamb),$(STACK_ROOT)/$(lamb))))
endef
