package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Locatable
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.services.SizeFinder
import uk.ac.wellcome.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.store
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

class S3FixityChecker(
  val streamReader: store.Readable[S3ObjectLocation, InputStreamWithLength],
  val sizeFinder: SizeFinder[S3ObjectLocation],
  val tags: Tags[S3ObjectLocation],
  val locator: Locatable[S3ObjectLocation, S3ObjectLocationPrefix, URI]
) extends FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix]
    with Logging

object S3FixityChecker {
  def apply()(implicit s3Client: AmazonS3) = {
    val streamReader = new S3StreamStore()
    val sizeFinder = new S3SizeFinder()
    val tags = new S3Tags()
    val locator = S3Locatable.s3UriLocatable

    new S3FixityChecker(streamReader, sizeFinder, tags, locator)
  }
}
