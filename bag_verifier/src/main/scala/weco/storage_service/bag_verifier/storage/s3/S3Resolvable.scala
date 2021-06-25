package weco.storage_service.bag_verifier.storage.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage.s3.S3ObjectLocation

class S3Resolvable(implicit s3Client: AmazonS3)
    extends Resolvable[S3ObjectLocation] {
  override def resolve(location: S3ObjectLocation): URI =
    s3Client.getUrl(location.bucket, location.key).toURI
}
