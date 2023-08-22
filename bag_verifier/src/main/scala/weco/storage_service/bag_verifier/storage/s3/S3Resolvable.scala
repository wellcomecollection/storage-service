package weco.storage_service.bag_verifier.storage.s3

import java.net.{URI, URLEncoder}

import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage.providers.s3.S3ObjectLocation

class S3Resolvable extends Resolvable[S3ObjectLocation] {
  override def resolve(location: S3ObjectLocation): URI = {
    val encodedKey =
      URLEncoder.encode(location.key, "UTF-8").replace("+", "%20")
    new URI(s"s3://${location.bucket}/$encodedKey")
  }
}
