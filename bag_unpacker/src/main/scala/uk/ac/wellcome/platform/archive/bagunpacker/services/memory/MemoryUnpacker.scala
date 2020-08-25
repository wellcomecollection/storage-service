package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import uk.ac.wellcome.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import uk.ac.wellcome.storage.store
import uk.ac.wellcome.storage.store.Writable
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class MemoryUnpacker()(implicit streamStore: MemoryStreamStore[MemoryLocation])
    extends Unpacker[MemoryLocation, MemoryLocation, MemoryLocationPrefix] {
  override protected val reader
    : store.Readable[MemoryLocation, InputStreamWithLength] =
    streamStore

  override protected val writer
    : Writable[MemoryLocation, InputStreamWithLength] =
    streamStore
}
