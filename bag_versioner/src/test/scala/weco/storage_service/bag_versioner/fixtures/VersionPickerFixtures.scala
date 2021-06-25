package weco.storage_service.bag_versioner.fixtures

import java.util.UUID

import cats.Id
import weco.fixtures.TestWith
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.bag_versioner.versioning.memory.MemoryIngestVersionManager
import weco.storage_service.bag_versioner.versioning.{
  IngestVersionManagerError,
  VersionPicker
}
import weco.storage.locking.{
  LockDao,
  LockFailure,
  LockingService,
  UnlockFailure
}
import weco.storage.locking.memory.MemoryLockDao

trait VersionPickerFixtures {

  def createBrokenLockDao: LockDao[String, UUID] = new LockDao[String, UUID] {
    override def lock(id: String, contextId: UUID): LockResult = Left(
      LockFailure(id, new Throwable("BOOM!"))
    )

    override def unlock(contextId: UUID): UnlockResult = Left(
      UnlockFailure(contextId, new Throwable("BOOM!"))
    )
  }

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
