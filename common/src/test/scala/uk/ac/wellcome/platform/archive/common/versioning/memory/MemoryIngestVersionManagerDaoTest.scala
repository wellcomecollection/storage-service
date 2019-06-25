package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, IngestVersionManagerDaoTestCases, VersionRecord}

class MemoryIngestVersionManagerDaoTest extends IngestVersionManagerDaoTestCases[MemoryIngestVersionManagerDao] {
  override def withContext[R](testWith: TestWith[MemoryIngestVersionManagerDao, R]): R =
    testWith(new MemoryIngestVersionManagerDao())

  override def withDao[R](initialRecords: Seq[VersionRecord])(testWith: TestWith[IngestVersionManagerDao, R])(implicit dao: MemoryIngestVersionManagerDao): R = {
    dao.records = dao.records ++ initialRecords

    testWith(dao)
  }
}
