ROOT = $(shell git rev-parse --show-toplevel)

include $(ROOT)/makefiles/functions.Makefile

PROJECT_ID = storage

STACK_ROOT 	= monitoring

SBT_APPS =
SBT_NO_DOCKER_APPS =

SBT_DOCKER_LIBRARIES    =
SBT_NO_DOCKER_LIBRARIES =

PYTHON_APPS =
LAMBDAS 	= end_to_end_bag_test

TF_NAME =
TF_PATH =

$(val $(call stack_setup))
