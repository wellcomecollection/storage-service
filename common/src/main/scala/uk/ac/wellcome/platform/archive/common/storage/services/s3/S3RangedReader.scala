package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.platform.archive.common.storage.models.{ByteRange, ClosedByteRange, OpenByteRange}
import uk.ac.wellcome.platform.archive.common.storage.services.RangedReader
import uk.ac.wellcome.storage.{ReadError, StoreReadError}
import uk.ac.wellcome.storage.s3.S3ObjectLocation

import scala.util.{Failure, Success, Try}

class S3RangedReader(implicit s3Client: AmazonS3) extends RangedReader[S3ObjectLocation] {
  override def getBytes(location: S3ObjectLocation, range: ByteRange): Either[ReadError, Array[Byte]] = Try {

    // The S3 Range request is *inclusive* of the boundaries.
    //
    // For example, if you read (start=0, end=5), you get bytes [0, 1, 2, 3, 4, 5].
    // We never want to read more than bufferSize bytes at a time.
    val getRequest = range match {
      case ClosedByteRange(start, count) =>
        new GetObjectRequest(location.bucket, location.key)
          .withRange(start, start + count - 1)

      case OpenByteRange(start) =>
        new GetObjectRequest(location.bucket, location.key)
          .withRange(start)
    }

    val s3InputStream = s3Client.getObject(getRequest).getObjectContent
    val byteArray = IOUtils.toByteArray(s3InputStream)

    // Remember to close the input stream afterwards, or we get errors like
    //
    //    com.amazonaws.SdkClientException: Unable to execute HTTP request:
    //    Timeout waiting for connection from pool
    //
    s3InputStream.close()

    byteArray
  } match {
    case Success(bytes) => Right(bytes)
    case Failure(err)   => Left(StoreReadError(err))
  }
}
