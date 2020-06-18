package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.S3Errors
import uk.ac.wellcome.storage.store.RetryableReadable
import uk.ac.wellcome.storage.store.memory.MemoryStore

trait SizeFinder extends RetryableReadable[Long] {
  override val maxRetries: Int = 3

  def getSize(location: ObjectLocation): Either[ReadError, Long] = {
    get(location) match {
      case Right(Identified(_, size)) => Right(size)
      case Left(err)                  => Left(err)
    }
  }
}

class MemorySizeFinder(
  memoryStore: MemoryStore[ObjectLocation, Array[Byte]]
) extends SizeFinder {

  override def retryableGetFunction(location: ObjectLocation): Long =
    memoryStore.entries.get(location) match {
      case Some(entry) => entry.length
      case None        => throw new Throwable(s"No such entry $location!")
    }

  override def buildGetError(throwable: Throwable): ReadError =
    throwable.getMessage match {
      case msg if msg.startsWith("No such entry") =>
        DoesNotExistError(throwable)
      case _ => StoreReadError(throwable)
    }
}

class S3SizeFinder(implicit s3Client: AmazonS3) extends SizeFinder {
  override def retryableGetFunction(location: ObjectLocation): Long = {
    s3Client
      .getObjectMetadata(location.namespace, location.path)
      .getContentLength
  }

  override def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable) match {
      case StoreReadError(exc: AmazonS3Exception)
        if exc.getMessage.startsWith("Not Found") =>
        DoesNotExistError(exc)

      case other => other
    }
}
