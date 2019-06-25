package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager

class MemoryIngestVersionManager(
  val dao: MemoryIngestVersionManagerDao = new MemoryIngestVersionManagerDao())
    extends IngestVersionManager
