package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.commons.io.FileUtils
import uk.ac.wellcome.platform.archive.bagunpacker.services.{Unpacker, UnpackerError, UnpackerStorageError}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class S3Unpacker(
  bufferSize: Long = 128 * FileUtils.ONE_MB
)(implicit s3Client: AmazonS3)
    extends Unpacker[S3ObjectLocation, S3ObjectLocation, S3ObjectLocationPrefix] {
  private val s3StreamStore = new S3StreamStore()

  val reader: S3StreamReader = new S3StreamReader(bufferSize = bufferSize)

  override def get(
    location: S3ObjectLocation
  ): Either[StorageError, InputStream] =
    reader.get(location).map { _.identifiedT }

  override def put(
    location: S3ObjectLocation
  )(inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    s3StreamStore
      .put(location)(inputStream)
      .map { _ =>
        ()
      }

  override def buildMessageFor(
    srcLocation: S3ObjectLocation,
    error: UnpackerError
  ): Option[String] =
    error match {

      // An Access Denied error from S3 could be (at least) two scenarios:
      //
      //    - The object exists, but it's in a bucket we don't have Get* permissions.
      //
      //    - The object doesn't exist, and it's in a bucket where we have Get* but not
      //      List* permissions.  In that case, S3 will give a generic error rather than give
      //      away information about what's in the bucket.
      //
      // In practice, we've seen the latter case a couple of times, so we want the user
      // to check the object really exists when they get this error, not chalk it up to
      // an IAM error that devs need to fix.  It *might* be something they can fix themselves.
      //
      case UnpackerStorageError(StoreReadError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("Access Denied") =>
        Some(
          s"Error reading $srcLocation: either it doesn't exist, or the unpacker doesn't have permission to read it"
        )

      case UnpackerStorageError(StoreReadError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("The specified bucket is not valid") =>
        Some(s"${srcLocation.bucket} is not a valid S3 bucket name")

      case UnpackerStorageError(DoesNotExistError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("The specified bucket does not exist") =>
        Some(s"There is no S3 bucket ${srcLocation.bucket}")

      case _ =>
        warn(s"Error unpacking bag at $srcLocation: $error")
        super.buildMessageFor(srcLocation, error)
    }
}
