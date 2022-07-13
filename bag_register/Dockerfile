FROM public.ecr.aws/docker/library/eclipse-temurin:11-jre-alpine

LABEL maintainer = "Wellcome Collection <digital@wellcomecollection.org>"

RUN apk add --no-cache bash

ADD target/universal/stage /opt/docker

ENTRYPOINT ["/opt/docker/bin/bag_register"]
