package uk.ac.wellcome.platform.archive.bag_register.services.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.bag_register.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStore}

class MemoryStorageManifestService(
  implicit store: MemoryStore[MemoryLocation, Array[Byte]]
) extends StorageManifestService[MemoryLocation] {

  override def toLocationPrefix(prefix: ObjectLocationPrefix): MemoryLocationPrefix =
    MemoryLocationPrefix(namespace = prefix.namespace, pathPrefix = prefix.path)

  override val sizeFinder: SizeFinder[MemoryLocation] =
    new MemorySizeFinder[MemoryLocation](store)

  override implicit val streamReader: MemoryStreamStore[MemoryLocation] =
    new MemoryStreamStore[MemoryLocation](store)

  override def createLocation(uri: URI): MemoryLocation =
    new MemoryLocation(
      namespace = uri.getHost,
      path = uri.getPath.stripPrefix("/")
    )
}
