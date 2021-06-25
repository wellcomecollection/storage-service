package weco.storage_service.bag_verifier.fixity.memory

import java.net.URI

import weco.storage_service.bag_verifier.fixity.FixityChecker
import weco.storage_service.bag_verifier.storage.{Locatable, LocateFailure}
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.services.SizeFinder
import weco.storage.services.memory.MemorySizeFinder
import weco.storage.store.memory.MemoryStreamStore
import weco.storage.tags.memory.MemoryTags

class MemoryFixityChecker(
  val streamReader: MemoryStreamStore[MemoryLocation],
  val tags: MemoryTags[MemoryLocation]
) extends FixityChecker[MemoryLocation, MemoryLocationPrefix] {

  override protected val sizeFinder: SizeFinder[MemoryLocation] =
    new MemorySizeFinder(streamReader.memoryStore)

  override implicit val locator
    : Locatable[MemoryLocation, MemoryLocationPrefix, URI] =
    new Locatable[MemoryLocation, MemoryLocationPrefix, URI] {
      override def locate(uri: URI)(
        maybeRoot: Option[MemoryLocationPrefix]
      ): Either[LocateFailure[URI], MemoryLocation] = Right(
        MemoryLocation(
          namespace = uri.getHost,
          path = uri.getPath.stripPrefix("/")
        )
      )
    }
}
