package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3LargeStreamReader
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class S3StreamReader(bufferSize: Long)(implicit s3Client: AmazonS3)
    extends Readable[S3ObjectLocation, InputStreamWithLength]
    with Logging {
  private val underlying = new S3LargeStreamReader(bufferSize = bufferSize)

  override def get(location: S3ObjectLocation): ReadEither =
    underlying.get(location)
}
