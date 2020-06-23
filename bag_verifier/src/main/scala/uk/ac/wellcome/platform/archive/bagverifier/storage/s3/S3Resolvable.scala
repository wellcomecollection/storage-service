package uk.ac.wellcome.platform.archive.bagverifier.storage.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.storage.ObjectLocation

class S3Resolvable(implicit s3Client: AmazonS3)
    extends Resolvable[ObjectLocation] {
  override def resolve(location: ObjectLocation): URI =
    s3Client.getUrl(location.namespace, location.path).toURI
}
