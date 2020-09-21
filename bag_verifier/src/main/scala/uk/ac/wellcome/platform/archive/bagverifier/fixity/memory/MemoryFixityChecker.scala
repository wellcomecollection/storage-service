package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.{Locatable, LocateFailure}
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import uk.ac.wellcome.storage.services.SizeFinder
import uk.ac.wellcome.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.tags.memory.MemoryTags

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
