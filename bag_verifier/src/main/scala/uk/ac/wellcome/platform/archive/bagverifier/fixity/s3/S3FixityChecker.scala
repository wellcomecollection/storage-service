package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.NewS3Tags

class S3FixityChecker(implicit s3Client: AmazonS3)
    extends FixityChecker[S3ObjectLocation]
    with Logging {

  import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable._
  import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Locatable._

  override protected val streamStore: StreamStore[S3ObjectLocation] =
    new NewS3StreamStore()

  override protected val sizeFinder: S3SizeFinder =
    new S3SizeFinder()

  override val tags: Tags[S3ObjectLocation] = new NewS3Tags()

  override def locate(uri: URI): Either[LocateFailure[URI], S3ObjectLocation] =
    uri.locate
}
