ACCOUNT_ID = 975596993436

ROOT = $(shell git rev-parse --show-toplevel)
include $(ROOT)/makefiles/functions.Makefile

nginx-build:
	$(call build_image,nginx,nginx/Dockerfile)

nginx-publish: nginx-build
	$(call publish_service,nginx,nginx,$(ACCOUNT_ID))
