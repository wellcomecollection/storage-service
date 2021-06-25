package weco.storage_service.bag_versioner.versioning.memory

import weco.storage_service.bag_versioner.versioning.IngestVersionManager

class MemoryIngestVersionManager(
  val dao: MemoryIngestVersionManagerDao = new MemoryIngestVersionManagerDao()
) extends IngestVersionManager
