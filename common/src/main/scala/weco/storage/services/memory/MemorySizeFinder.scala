package weco.storage.services.memory

import weco.storage.services.SizeFinder
import weco.storage.store.memory.MemoryStore
import weco.storage.{DoesNotExistError, Identified}

class MemorySizeFinder[Ident](
  memoryStore: MemoryStore[Ident, Array[Byte]]
) extends SizeFinder[Ident] {
  override def get(id: Ident): ReadEither =
    memoryStore.entries.get(id) match {
      case Some(entry) => Right(Identified(id, entry.length))
      case None =>
        Left(DoesNotExistError(new Throwable(s"There is no entry for id=$id")))
    }
}
