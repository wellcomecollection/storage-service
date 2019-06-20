package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, UpdateIngestType}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.VersionPickerFixtures
import uk.ac.wellcome.storage.{LockDao, LockFailure, UnlockFailure}

import scala.util.{Failure, Success}

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

      // Pick an initial version
      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.ofEpochSecond(1)
      )

      // Now assign some more versions with different
      // ingest IDs.
      (2 to 4).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(t)
        )
      }

      // Now assign another version, this time with a fixed ingest ID.
      val result = picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestType = UpdateIngestType,
        ingestDate = Instant.ofEpochSecond(100)
      )

      // If we keep asking for a version with the same ingest ID, we
      // get the same ones back each time
      (1 to 5).foreach { _ =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(100)
        ) shouldBe result
      }
    }
  }

  it("picks monotonically increasing versions for an external identifier") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier

      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.ofEpochSecond(1)
      ) shouldBe Success(1)

      (2 to 5).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
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
    val lockDao = createLockDao

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

  describe("checking the ingest type") {
    it("only allows ingest type 'create' once") {
      val externalIdentifier = createExternalIdentifier

      withVersionPicker { picker =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = CreateIngestType,
          ingestDate = Instant.now()
        )

        val result = picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = CreateIngestType,
          ingestDate = Instant.now()
        )

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[IllegalVersionAssignment]
        result.failed.get.getMessage should startWith("Ingest type 'create' is not allowed")
      }
    }

    it("only allows ingest type 'update' on an already-existing bag") {
      val externalIdentifier = createExternalIdentifier

      withVersionPicker { picker =>
        val result = picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
          ingestDate = Instant.now()
        )

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[IllegalVersionAssignment]
        result.failed.get.getMessage should startWith("Ingest type 'update' is not allowed")
      }
    }
  }
}
