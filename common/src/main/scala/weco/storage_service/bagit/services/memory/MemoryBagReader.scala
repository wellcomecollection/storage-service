package weco.storage_service.bagit.services.memory

import weco.storage_service.bagit.services.BagReader
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.store.memory.MemoryStreamStore

class MemoryBagReader()(
  implicit val readable: MemoryStreamStore[MemoryLocation]
) extends BagReader[MemoryLocation, MemoryLocationPrefix]
