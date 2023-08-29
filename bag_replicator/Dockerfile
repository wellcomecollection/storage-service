# Note: in our other apps we use the `11-jre-alpine` tag, but we need
# the `11-jre` tag here.  If you try to run this container with the
# Alpine base image, you get an error:
#
#     java.lang.UnsatisfiedLinkError: /tmp/AWSCRT_[…]_libaws-crt-jni.so:
#     Error loading shared library ld-linux-x86-64.so.2:
#     No such file or directory (needed by /tmp/AWSCRT_[…]_libaws-crt-jni.so)
#
# The AWS CRT library is used by the S3Transfer class, which is only
# used in this app; it allows us to do multi-part copies of objects,
# which we need to copy objects which are >5GB in size.
#
# This error message is the AWS CRT library trying to bind to a glibc
# version of OpenSSL, but Alpine Linux uses musl instead.  The `11-jre`
# image is using glibc, like most other distros, so switching to it
# should fix this issue.
#
# See https://github.com/wellcomecollection/storage-service/issues/1066
# See https://github.com/vercel/next.js/issues/30713

FROM public.ecr.aws/docker/library/eclipse-temurin:11-jre

LABEL maintainer = "Wellcome Collection <digital@wellcomecollection.org>"

ADD target/universal/stage /opt/docker

ENTRYPOINT ["/opt/docker/bin/bag_replicator"]
