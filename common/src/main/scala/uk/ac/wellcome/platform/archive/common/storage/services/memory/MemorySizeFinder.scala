package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage.{DoesNotExistError, Identified}
import uk.ac.wellcome.storage.store.memory.MemoryStore

class MemorySizeFinder[Ident](
  memoryStore: MemoryStore[Ident, Array[Byte]]
) extends SizeFinder[Ident] {
  override def get(id: Ident): ReadEither = {
    println(s"@@AWLC getting id = $id")
      memoryStore.entries.get(id) match {
      case Some(entry) => Right(Identified(id, entry.length))
      case None        => Left(DoesNotExistError())
    }
  }
}
