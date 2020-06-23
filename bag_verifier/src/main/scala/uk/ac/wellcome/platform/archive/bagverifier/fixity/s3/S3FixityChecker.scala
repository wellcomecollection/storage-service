package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags
import uk.ac.wellcome.storage.{ObjectLocation, S3ObjectLocation}

class S3FixityChecker(implicit s3Client: AmazonS3)
    extends FixityChecker[S3ObjectLocation]
    with Logging {

  import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable._
  import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Locatable._

  override protected val streamStore: StreamStore[ObjectLocation] =
    new S3StreamStore()

  override protected val sizeFinder: S3SizeFinder =
    new S3SizeFinder()

  override val tags: Tags[ObjectLocation] = new S3Tags()

  override def locate(uri: URI): Either[LocateFailure[URI], S3ObjectLocation] =
    uri.locate

  // TODO: Bridging code while we split ObjectLocation.  Remove this later.
  // See https://github.com/wellcomecollection/platform/issues/4596
  override def toLocation(s3Location: S3ObjectLocation): ObjectLocation =
    s3Location.toObjectLocation
}
