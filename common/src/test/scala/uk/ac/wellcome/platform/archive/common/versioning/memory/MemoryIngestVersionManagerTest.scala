package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManager, IngestVersionManagerTestCases}

class MemoryIngestVersionManagerTest extends IngestVersionManagerTestCases[MemoryIngestVersionManagerDao] {
  override def withContext[R](testWith: TestWith[MemoryIngestVersionManagerDao, R]): R =
    testWith(new MemoryIngestVersionManagerDao())

  override def withManager[R](testWith: TestWith[IngestVersionManager, R])(implicit context: MemoryIngestVersionManagerDao): R =
    testWith(new MemoryIngestVersionManager(context))
}
