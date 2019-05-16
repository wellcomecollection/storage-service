package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManager,
  IngestVersionManagerDao,
  MemoryIngestVersionManagerDao
}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.VersionPickerFixtures
import uk.ac.wellcome.storage.fixtures.InMemoryLockDao
import uk.ac.wellcome.storage.{
  LockDao,
  LockFailure,
  LockingService,
  UnlockFailure
}

import scala.util.{Failure, Success, Try}

class VersionPickerTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with VersionPickerFixtures {
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
        result.failed.get.getMessage should startWith(
          "Latest version has a newer ingest date")
      }
    }
  }

  it("locks around the ingest ID and external identifiers") {
    val lockDao = new InMemoryLockDao()

    withVersionPicker(lockDao) { picker =>
      val ingestId = createIngestID
      val externalIdentifier = createExternalIdentifier

      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = Instant.now()
      )

      lockDao.getCurrentLocks shouldBe empty
      lockDao.history.map { _.id } should contain theSameElementsAs List(
        s"ingest:$ingestId",
        s"external:$externalIdentifier"
      )
    }
  }

  it("fails if the locking dao fails") {
    val lockDao = new LockDao[String, UUID] {
      override def lock(id: String, contextId: UUID): LockResult = Left(
        LockFailure(id, new Throwable("BOOM!"))
      )

      override def unlock(contextId: UUID): UnlockResult = Left(
        UnlockFailure(contextId, new Throwable("BOOM!"))
      )
    }

    withVersionPicker(lockDao) { picker =>
      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now()
      )

      result shouldBe a[Failure[_]]
      result.failed.get shouldBe a[RuntimeException]
      result.failed.get.getMessage should startWith("Locking error:")
    }
  }
}
