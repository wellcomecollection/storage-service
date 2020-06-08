package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.storage.services.{
  MemorySizeFinder,
  SizeFinder
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class MemoryFixityChecker(
  val streamStore: MemoryStreamStore[ObjectLocation],
  val tags: MemoryTags[ObjectLocation]
) extends FixityChecker {
  override protected val sizeFinder: SizeFinder =
    new MemorySizeFinder(streamStore.memoryStore)

  override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
    Right(
      ObjectLocation(
        namespace = uri.getHost,
        path = uri.getPath.stripPrefix("/")
      )
    )
}
