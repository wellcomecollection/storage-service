package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManager, IngestVersionManagerDao, MemoryIngestVersionManagerDao}
import uk.ac.wellcome.storage.fixtures.InMemoryLockDao
import uk.ac.wellcome.storage.{LockDao, LockingService}

import scala.util.{Success, Try}

class VersionPickerTest extends FunSpec with Matchers with ExternalIdentifierGenerators {
  def withVersionPicker[R](testWith: TestWith[VersionPicker, R]): R =
    withVersionPicker(new InMemoryLockDao()) { picker =>
      testWith(picker)
    }

  def withVersionPicker[R](dao: LockDao[String, UUID])(testWith: TestWith[VersionPicker, R]): R = {
    val lockingService = new LockingService[Int, Try, LockDao[String, UUID]] {
      override implicit val lockDao: LockDao[String, UUID] = dao

      override protected def createContextId(): UUID = UUID.randomUUID()
    }

    val ingestVersionManager = new IngestVersionManager {
      override val dao: IngestVersionManagerDao = new MemoryIngestVersionManagerDao()
    }

    val picker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager = ingestVersionManager
    )

    testWith(picker)
  }

  it("assigns version 1 if it hasn't seen this external ID before") {
    withVersionPicker { picker =>
      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now()
      )

      result shouldBe Success(1)
    }
  }
}
