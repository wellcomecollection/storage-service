package weco.storage.transfer.azure

import weco.storage.s3.S3ObjectLocation

case class SourceS3Object(
  location: S3ObjectLocation,
  size: Long
)
