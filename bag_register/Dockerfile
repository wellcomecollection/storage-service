FROM public.ecr.aws/docker/library/openjdk:11

LABEL maintainer = "Wellcome Collection <digital@wellcomecollection.org>"

ADD target/universal/stage /opt/docker

ENTRYPOINT ["/opt/docker/bin/bag_register"]
