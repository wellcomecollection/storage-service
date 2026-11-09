FROM public.ecr.aws/docker/library/eclipse-temurin:11-jre-alpine

LABEL maintainer = "Wellcome Collection <digital@wellcomecollection.org>"

# Pick up Alpine security fixes the base image has not been rebuilt with yet.
RUN apk upgrade --no-cache && apk add --no-cache bash

ADD target/universal/stage /opt/docker

ENTRYPOINT ["/opt/docker/bin/bag_tagger"]
