package uk.ac.wellcome.platform.archive.common.bagit.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class MemoryBagReader()(
  implicit val streamStore: MemoryStreamStore[ObjectLocation]
) extends BagReader
