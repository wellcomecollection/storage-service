package uk.ac.wellcome.platform.archive.common.bagit.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class MemoryBagReader()(
  implicit val readable: MemoryStreamStore[MemoryLocation]
) extends BagReader[MemoryLocation, MemoryLocationPrefix]
