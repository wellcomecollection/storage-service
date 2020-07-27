package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.storage.providers.memory.MemoryLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class MemoryFixityChecker(
  val streamStore: MemoryStreamStore[MemoryLocation],
  val tags: MemoryTags[MemoryLocation]
) extends FixityChecker[MemoryLocation] {

  override protected val sizeFinder: SizeFinder[MemoryLocation] =
    new MemorySizeFinder(streamStore.memoryStore)

  override def locate(uri: URI): Either[LocateFailure[URI], MemoryLocation] =
    Right(
      MemoryLocation(
        namespace = uri.getHost,
        path = uri.getPath.stripPrefix("/")
      )
    )
}
