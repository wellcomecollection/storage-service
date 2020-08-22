package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.s3.S3RangedReader
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.{S3Errors, S3ObjectLocation}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

/** If you hold open an S3ObjectInputStream for a long time, eventually the
  * network times out and you get an error complaining that it hasn't read
  * all the bytes from the stream.  For example:
  *
  *   com.amazonaws.SdkClientException: Data read has a different length than the expected:
  *   dataLength=1234; expectedLength=56789
  *
  * To get around this issue, we read large objects in "chunks", making a
  * separate GetObject request for each segment.  The individual streams are then
  * stitched back together in a SequenceInputStream, so to the caller it's
  * presented as a single continuous stream.
  *
  * We're hoping that making regular GetObject requests will keep the
  * network "fresh", and reduce the risk of a timeout while we're reading
  * the stream.
  *
  */
class S3StreamReader(bufferSize: Long)(implicit s3Client: AmazonS3)
    extends Readable[S3ObjectLocation, InputStreamWithLength]
    with Logging {

  private val rangedReader = new S3RangedReader()

  private class S3StreamEnumeration(
    location: S3ObjectLocation,
    contentLength: Long,
    bufferSize: Long
  ) extends util.Enumeration[InputStream] {
    var currentPosition = 0L
    val totalLength: Long = contentLength

    override def hasMoreElements: Boolean =
      currentPosition < totalLength

    override def nextElement(): InputStream = {
      val byteArray = rangedReader.getBytes(
        location = location,
        offset = currentPosition,
        count = bufferSize,
        totalLength = totalLength
      )

      currentPosition += bufferSize

      new ByteArrayInputStream(byteArray)
    }
  }

  override def get(location: S3ObjectLocation): ReadEither =
    Try {
      // We use getObject here rather than getObjectMetadata so we get more
      // detailed errors from S3 about why a GetObject fails, if necessary.
      //
      // e.g. GetObject will return "The bucket name was invalid" rather than
      // "Bad Request".
      //
      val s3Object = s3Client.getObject(location.bucket, location.key)
      val metadata = s3Object.getObjectMetadata
      val contentLength = metadata.getContentLength

      // Abort the stream to avoid getting a warning:
      //
      //    Not all bytes were read from the S3ObjectInputStream, aborting
      //    HTTP connection. This is likely an error and may result in
      //    sub-optimal behavior.
      //
      s3Object.getObjectContent.abort()
      s3Object.getObjectContent.close()

      val streams = new S3StreamEnumeration(
        location,
        contentLength = contentLength,
        bufferSize = bufferSize
      )

      new InputStreamWithLength(
        new SequenceInputStream(streams),
        length = contentLength
      )
    } match {
      case Success(inputStream) => Right(Identified(location, inputStream))
      case Failure(err)         => Left(S3Errors.readErrors(err))
    }
}
