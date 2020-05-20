ACCOUNT_ID = 975596993436

include makefiles/functions.Makefile
include makefiles/formatting.Makefile
include api/Makefile
include indexer/Makefile
include ingests/Makefile
include monitoring/Makefile
include nginx/Makefile
include python_client/Makefile

PROJECT_ID = storage

STACK_ROOT 	= .

SBT_APPS = notifier \
           bag_register \
           bag_replicator \
		   bag_root_finder \
           bag_verifier \
           bag_unpacker \
           bag_versioner \
           replica_aggregator
SBT_NO_DOCKER_APPS =

SBT_DOCKER_LIBRARIES    = common
SBT_NO_DOCKER_LIBRARIES = bags_common display

PYTHON_APPS =
LAMBDAS 	= s3_object_tagger

TF_NAME = storage
TF_PATH = $(STACK_ROOT)/terraform

$(val $(call stack_setup))