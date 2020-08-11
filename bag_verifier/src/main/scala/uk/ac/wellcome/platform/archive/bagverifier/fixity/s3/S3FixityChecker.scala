package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Locatable
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

class S3FixityChecker(implicit s3Client: AmazonS3)
    extends FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix]
    with Logging {

  override protected val streamStore: StreamStore[S3ObjectLocation] =
    new S3StreamStore()

  override protected val sizeFinder: S3SizeFinder =
    new S3SizeFinder()

  override val tags: Tags[S3ObjectLocation] = new S3Tags()
  override implicit val locator
    : Locatable[S3ObjectLocation, S3ObjectLocationPrefix, URI] =
    S3Locatable.s3UriLocatable
}
