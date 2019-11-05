package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStoreEntry}

import scala.util.Try

trait SizeFinder {
  def getSize(location: ObjectLocation): Try[Long]
}

class MemorySizeFinder(
  memoryStore: MemoryStore[ObjectLocation, MemoryStreamStoreEntry]
) extends SizeFinder {
  override def getSize(location: ObjectLocation): Try[Long] = Try {
    memoryStore.entries
      .getOrElse(location, throw new Throwable(s"No such entry $location!"))
      .bytes
      .length
  }
}

class S3SizeFinder(implicit s3Client: AmazonS3) extends SizeFinder {
  def getSize(location: ObjectLocation): Try[Long] = Try {
    s3Client
    .getObjectMetadata(location.namespace, location.path)
    .getContentLength
  }
}
