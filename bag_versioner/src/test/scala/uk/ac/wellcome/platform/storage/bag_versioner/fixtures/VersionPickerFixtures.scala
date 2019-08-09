package uk.ac.wellcome.platform.storage.bag_versioner.fixtures

import java.util.UUID

import cats.Id
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManagerError
import uk.ac.wellcome.platform.archive.common.versioning.memory.MemoryIngestVersionManager
import uk.ac.wellcome.platform.storage.bag_versioner.versioning.VersionPicker
import uk.ac.wellcome.storage.locking.{LockDao, LockingService}
import uk.ac.wellcome.storage.locking.memory.MemoryLockDao

trait VersionPickerFixtures {
  def createLockDao: MemoryLockDao[String, UUID] =
    new MemoryLockDao[String, UUID] {}

  def withVersionPicker[R](testWith: TestWith[VersionPicker, R]): R =
    withVersionPicker(createLockDao) { picker =>
      testWith(picker)
    }

  def withVersionPicker[R](
    dao: LockDao[String, UUID]
  )(testWith: TestWith[VersionPicker, R]): R = {
    val lockingService = new LockingService[
      Either[IngestVersionManagerError, BagVersion],
      Id,
      LockDao[String, UUID]
    ] {
      override implicit val lockDao: LockDao[String, UUID] = dao

      override protected def createContextId(): UUID = UUID.randomUUID()
    }

    val ingestVersionManager = new MemoryIngestVersionManager()

    val picker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager = ingestVersionManager
    )

    testWith(picker)
  }
}
