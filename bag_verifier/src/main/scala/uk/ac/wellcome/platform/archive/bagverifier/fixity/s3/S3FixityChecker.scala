package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.{Identified, ObjectLocation, S3ObjectLocation}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

class S3FixityChecker(implicit s3Client: AmazonS3)
    extends FixityChecker
    with Logging {

  import uk.ac.wellcome.platform.archive.common.storage.Locatable._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3LocatableInstances._

  override protected val streamStore: StreamStore[ObjectLocation] =
    new S3StreamStore()

  // TODO: This is a temporary wrapper while we migrate to ObjectLocation.  Remove it.
  implicit val s3SizeFinder: S3SizeFinder = new S3SizeFinder()

  override protected val sizeFinder: SizeFinder[ObjectLocation] =
    new SizeFinder[ObjectLocation] {
      override def get(location: ObjectLocation): ReadEither =
        s3SizeFinder
          .get(S3ObjectLocation(location))
          .map { case Identified(_, size) => Identified(location, size) }
    }

  override val tags: Tags[ObjectLocation] = new S3Tags()

  override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
    uri.locate
}
