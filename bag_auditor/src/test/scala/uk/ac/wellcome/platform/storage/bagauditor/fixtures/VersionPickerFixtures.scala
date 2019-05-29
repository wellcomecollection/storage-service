package uk.ac.wellcome.platform.storage.bagauditor.fixtures

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManager,
  IngestVersionManagerDao,
  MemoryIngestVersionManagerDao
}
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.{LockDao, LockingService}
import uk.ac.wellcome.storage.memory.MemoryLockDao

import scala.util.Try

trait VersionPickerFixtures {
  def createLockDao: MemoryLockDao[String, UUID] =
    new MemoryLockDao[String, UUID] {}

  def withVersionPicker[R](testWith: TestWith[VersionPicker, R]): R =
    withVersionPicker(createLockDao) { picker =>
      testWith(picker)
    }

  def withVersionPicker[R](dao: LockDao[String, UUID])(
    testWith: TestWith[VersionPicker, R]): R = {
    val lockingService = new LockingService[Int, Try, LockDao[String, UUID]] {
      override implicit val lockDao: LockDao[String, UUID] = dao

      override protected def createContextId(): UUID = UUID.randomUUID()
    }

    val ingestVersionManager = new IngestVersionManager {
      override val dao: IngestVersionManagerDao =
        new MemoryIngestVersionManagerDao()
    }

    val picker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager = ingestVersionManager
    )

    testWith(picker)
  }
}
