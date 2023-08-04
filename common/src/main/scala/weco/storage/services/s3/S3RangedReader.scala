package weco.storage.services.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import weco.storage.ReadError
import weco.storage.models.{ByteRange, ClosedByteRange, OpenByteRange}
import weco.storage.s3.{S3Errors, S3ObjectLocation}
import weco.storage.services.RangedReader

import scala.util.{Failure, Success, Try}

class S3RangedReader(implicit s3Client: S3Client)
    extends RangedReader[S3ObjectLocation] {
  override def getBytes(
    location: S3ObjectLocation,
    range: ByteRange
  ): Either[ReadError, Array[Byte]] =
    Try {

      // The S3 Range request is *inclusive* of the boundaries.
      //
      // For example, if you read (start=0, end=5), you get bytes [0, 1, 2, 3, 4, 5].
      // We never want to read more than bufferSize bytes at a time.
      //
      // This uses the syntax of the Range HTTP header.  See:
      // https://docs.aws.amazon.com/AmazonS3/latest/userguide/range-get-olap.html
      // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
      val rangeHeader = range match {
        case ClosedByteRange(start, count) =>
          s"bytes=$start-${start + count - 1}"

        case OpenByteRange(start) =>
          s"bytes=$start-"
      }

      val getRequest =
        GetObjectRequest
          .builder()
          .bucket(location.bucket)
          .key(location.key)
          .range(rangeHeader)
          .build()

      s3Client
        .getObjectAsBytes(getRequest)
        .asByteArray()
    } match {
      case Success(bytes) => Right(bytes)
      case Failure(err)   => Left(S3Errors.readErrors(err))
    }
}
