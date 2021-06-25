package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import weco.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import weco.storage.store
import weco.storage.store.Writable
import weco.storage.store.memory.MemoryStreamStore
import weco.storage.streaming.InputStreamWithLength

class MemoryUnpacker()(implicit streamStore: MemoryStreamStore[MemoryLocation])
    extends Unpacker[MemoryLocation, MemoryLocation, MemoryLocationPrefix] {
  override protected val reader
    : store.Readable[MemoryLocation, InputStreamWithLength] =
    streamStore

  override protected val writer
    : Writable[MemoryLocation, InputStreamWithLength] =
    streamStore
}
