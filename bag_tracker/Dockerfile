FROM 760097843905.dkr.ecr.eu-west-1.amazonaws.com/openjdk:11

LABEL maintainer = "Wellcome Collection <digital@wellcomecollection.org>"

ADD target/universal/stage /opt/docker

ENTRYPOINT ["/opt/docker/bin/bag_tracker"]
