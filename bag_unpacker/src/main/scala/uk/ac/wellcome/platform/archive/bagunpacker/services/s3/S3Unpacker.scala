package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerError,
  UnpackerStorageError
}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{
  InputStreamWithLength,
  InputStreamWithLengthAndMetadata
}
import uk.ac.wellcome.storage.{
  DoesNotExistError,
  ObjectLocation,
  StorageError,
  StoreReadError
}

class S3Unpacker()(implicit s3Client: AmazonS3) extends Unpacker {
  private val s3StreamStore = new S3StreamStore()

  override def get(
    location: ObjectLocation
  ): Either[StorageError, InputStream] =
    s3StreamStore.get(location).map { _.identifiedT }

  override def put(
    location: ObjectLocation
  )(inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    s3StreamStore
      .put(location)(
        InputStreamWithLengthAndMetadata(inputStream, metadata = Map.empty)
      )
      .map { _ =>
        ()
      }

  override def formatLocation(location: ObjectLocation): String =
    s"s3://$location"

  override def buildMessageFor(
    srcLocation: ObjectLocation,
    error: UnpackerError
  ): Option[String] =
    error match {
      case UnpackerStorageError(StoreReadError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("Access Denied") =>
        Some(
          s"Access denied while trying to read ${formatLocation(srcLocation)}"
        )

      case UnpackerStorageError(StoreReadError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("The specified bucket is not valid") =>
        Some(s"${srcLocation.namespace} is not a valid S3 bucket name")

      case UnpackerStorageError(DoesNotExistError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("The specified bucket does not exist") =>
        Some(s"There is no S3 bucket ${srcLocation.namespace}")

      case _ =>
        warn(s"Error unpacking bag at $srcLocation: $error")
        super.buildMessageFor(srcLocation, error)
    }
}
