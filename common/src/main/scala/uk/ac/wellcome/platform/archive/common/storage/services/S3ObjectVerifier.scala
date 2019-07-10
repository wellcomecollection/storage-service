package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier[InputStreamWithLengthAndMetadata]
    with Logging {

  import uk.ac.wellcome.platform.archive.common.storage.Locatable._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3LocatableInstances._

  override protected val streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata] =
    new S3StreamStore()

  override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
    uri.locate
}
