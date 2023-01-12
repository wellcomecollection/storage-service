package weco.storage_service.bag_verifier.storage.s3

import java.net.URI

import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage.s3.S3ObjectLocation

class S3Resolvable extends Resolvable[S3ObjectLocation] {
  override def resolve(location: S3ObjectLocation): URI =
    new URI(s"s3://${location.bucket}/${location.key}")
}
