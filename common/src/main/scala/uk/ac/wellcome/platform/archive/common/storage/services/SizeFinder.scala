package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage.s3.S3Errors
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.{
  DoesNotExistError,
  ObjectLocation,
  ReadError,
  StoreReadError
}

import scala.util.{Failure, Success, Try}

trait SizeFinder {
  def getSize(location: ObjectLocation): Either[ReadError, Long]
}

class MemorySizeFinder(
  memoryStore: MemoryStore[ObjectLocation, Array[Byte]]
) extends SizeFinder {
  override def getSize(location: ObjectLocation): Either[ReadError, Long] =
    memoryStore.entries.get(location) match {
      case Some(entry) => Right(entry.length)
      case None =>
        Left(DoesNotExistError(new Throwable(s"No such entry $location!")))
    }
}

class S3SizeFinder(implicit s3Client: AmazonS3) extends SizeFinder {
  def getSize(location: ObjectLocation): Either[ReadError, Long] = {

    // TODO: Consider this being a "metadata readable"
    // note this used to retry
    val result = Try {
      s3Client
        .getObjectMetadata(location.namespace, location.path)
        .getContentLength
    } match {
      case Success(value)     => Right(value)
      case Failure(exception) => Left(S3Errors.readErrors(exception))
    }

    // The "Not Found" error message from GetObjectMetadata is different from
    // that for GetObject, so S3Get doesn't know to wrap it as a DoesNotExistError.
    //
    // TODO: Upstream this change into scala-storage.
    result match {
      case Left(StoreReadError(exc: AmazonS3Exception))
          if exc.getMessage.startsWith("Not Found") =>
        Left(DoesNotExistError(exc))

      case other => other
    }
  }
}
