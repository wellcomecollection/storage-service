package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManager, IngestVersionManagerDao, MemoryIngestVersionManagerDao}
import uk.ac.wellcome.storage.fixtures.InMemoryLockDao
import uk.ac.wellcome.storage.{LockDao, LockingService}

import scala.util.{Failure, Success, Try}

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

  it("always picks the same version for a given ingest ID") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier
      val ingestId = createIngestID

      (1 to 3).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(t)
        )
      }

      val result = picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = Instant.ofEpochSecond(100)
      )

      (1 to 3).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(t)
        )
      }

      (1 to 5).foreach { _ =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = Instant.ofEpochSecond(100)
        ) shouldBe result
      }
    }
  }

  it("picks monotonically increasing versions for an external identifier") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier

      (1 to 4).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(t)
        ) shouldBe Success(t)
      }
    }
  }

  it("fails if the ingest date goes backwards") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier
      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now()
      )

      (1 to 3).map { t =>
        val result = picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(t)
        )

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[RuntimeException]
        result.failed.get.getMessage should startWith("Latest version has a newer ingest date")
      }
    }
  }
}
