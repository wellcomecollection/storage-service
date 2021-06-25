package weco.storage_service.bag_verifier.fixity.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import org.apache.commons.io.FileUtils
import weco.storage_service.bag_verifier.fixity.FixityChecker
import weco.storage_service.bag_verifier.storage.Locatable
import weco.storage_service.bag_verifier.storage.s3.S3Locatable
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.services.SizeFinder
import weco.storage.services.s3.{S3LargeStreamReader, S3SizeFinder}
import weco.storage.store
import weco.storage.streaming.InputStreamWithLength
import weco.storage.tags.Tags
import weco.storage.tags.s3.S3Tags

class S3FixityChecker(
  val streamReader: store.Readable[S3ObjectLocation, InputStreamWithLength],
  val sizeFinder: SizeFinder[S3ObjectLocation],
  val tags: Tags[S3ObjectLocation],
  val locator: Locatable[S3ObjectLocation, S3ObjectLocationPrefix, URI]
) extends FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix]
    with Logging

object S3FixityChecker {
  def apply()(implicit s3Client: AmazonS3) = {
    val streamReader = new S3LargeStreamReader(
      bufferSize = 128 * FileUtils.ONE_MB
    )
    val sizeFinder = new S3SizeFinder()
    val tags = new S3Tags()
    val locator = S3Locatable.s3UriLocatable

    new S3FixityChecker(streamReader, sizeFinder, tags, locator)
  }
}
