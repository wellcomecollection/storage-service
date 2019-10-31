package uk.ac.wellcome.platform.storage.bag_versioner.versioning.memory

import uk.ac.wellcome.platform.storage.bag_versioner.versioning.IngestVersionManager

class MemoryIngestVersionManager(
  val dao: MemoryIngestVersionManagerDao = new MemoryIngestVersionManagerDao()
) extends IngestVersionManager
