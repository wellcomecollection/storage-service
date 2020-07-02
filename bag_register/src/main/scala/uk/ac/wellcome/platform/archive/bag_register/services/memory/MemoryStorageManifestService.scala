package uk.ac.wellcome.platform.archive.bag_register.services.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.bag_register.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class MemoryStorageManifestService(
  implicit
  val streamReader: MemoryStreamStore[MemoryLocation],
  val sizeFinder: SizeFinder[MemoryLocation]
) extends StorageManifestService[MemoryLocation] {

  override def toLocationPrefix(
    prefix: ObjectLocationPrefix
  ): MemoryLocationPrefix =
    MemoryLocationPrefix(namespace = prefix.namespace, pathPrefix = prefix.path)

  override def createLocation(uri: URI): MemoryLocation =
    new MemoryLocation(
      namespace = uri.getHost,
      path = uri.getPath.stripPrefix("/")
    )
}

object MemoryStorageManifestService {
  def apply()(
    implicit streamReader: MemoryStreamStore[MemoryLocation]
  ): MemoryStorageManifestService = {
    implicit val sizeFinder: MemorySizeFinder[MemoryLocation] =
      new MemorySizeFinder[MemoryLocation](streamReader.memoryStore)

    new MemoryStorageManifestService()
  }
}
