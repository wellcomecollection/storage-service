package uk.ac.wellcome.platform.archive.bagverifier.storage.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.storage.S3ObjectLocation

class S3Resolvable(implicit s3Client: AmazonS3)
    extends Resolvable[S3ObjectLocation] {
  override def resolve(location: S3ObjectLocation): URI =
    s3Client.getUrl(location.bucket, location.key).toURI
}
