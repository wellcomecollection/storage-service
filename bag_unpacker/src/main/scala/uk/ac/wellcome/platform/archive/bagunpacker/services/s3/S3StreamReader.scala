package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, GetObjectRequest}
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage._
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
      val getRequest =
        new GetObjectRequest(location.bucket, location.key)

      // The S3 Range request is *inclusive* of the boundaries.
      //
      // For example, if you read (start=0, end=5), you get bytes [0, 1, 2, 3, 4, 5].
      // We never want to read more than bufferSize bytes at a time.
      val requestWithRange =
        if (currentPosition + bufferSize >= totalLength) {
          debug(s"Reading $location: $currentPosition-[end] / $contentLength")
          getRequest.withRange(currentPosition)
        } else {
          debug(
            s"Reading $location: $currentPosition-${currentPosition + bufferSize - 1} / $contentLength"
          )
          getRequest.withRange(
            currentPosition,
            currentPosition + bufferSize - 1
          )
        }

      currentPosition += bufferSize

      // Remember to close the input stream afterwards, or we get errors like
      //
      //    com.amazonaws.SdkClientException: Unable to execute HTTP request:
      //    Timeout waiting for connection from pool
      //
      val s3InputStream = s3Client.getObject(requestWithRange).getObjectContent
      val byteArray = IOUtils.toByteArray(s3InputStream)
      s3InputStream.close()

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
      val getRequest = new GetObjectRequest(location.bucket, location.key)

      val s3Object = s3Client.getObject(getRequest)
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
      case Failure(err)         => Left(buildGetError(err))
    }

  private def buildGetError(throwable: Throwable): ReadError =
    throwable match {
      case exc: AmazonS3Exception if exc.getMessage.startsWith("Not Found") =>
        DoesNotExistError(exc)
      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("The specified key does not exist") =>
        DoesNotExistError(exc)
      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("The specified bucket does not exist") =>
        DoesNotExistError(exc)
      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("The specified bucket is not valid") =>
        StoreReadError(exc)
      case _ => StoreReadError(throwable)
    }
}
