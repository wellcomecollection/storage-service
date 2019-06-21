package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, UpdateIngestType}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.VersionPickerFixtures
import uk.ac.wellcome.platform.storage.bagauditor.models.{IngestTypeCreateForExistingBag, IngestTypeUpdateForNewBag, InternalVersionPickerError, VersionPickerError}
import uk.ac.wellcome.storage.{LockDao, LockFailure, UnlockFailure}

class VersionPickerTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with VersionPickerFixtures
    with EitherValues {

  it("assigns version 1 if it hasn't seen this external ID before") {
    withVersionPicker { picker =>
      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now()
      )

      result.right.value shouldBe 1
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
      ).right.value shouldBe 1

      (2 to 5).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(t)
        ).right.value shouldBe t
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

        result.left.value shouldBe a[InternalVersionPickerError]

        val err = result.left.value.asInstanceOf[InternalVersionPickerError]
        err.e.getMessage should startWith("Latest version has a newer ingest date")
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
      val result: Either[VersionPickerError, Int] = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now()
      )

      result.left.value shouldBe a[InternalVersionPickerError]

      val err = result.left.value.asInstanceOf[InternalVersionPickerError]
      err.e.getMessage should startWith("Locking error:")
    }
  }

  it("errors if there's an existing ingest with the wrong external identifier") {
    withVersionPicker { picker =>
      val ingestId = createIngestID

      picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = ingestId,
        ingestDate = Instant.now()
      )

      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = ingestId,
        ingestDate = Instant.now()
      )

      result.left.value shouldBe a[InternalVersionPickerError]

      val err = result.left.value.asInstanceOf[InternalVersionPickerError]
      err.e.getMessage should startWith("External identifiers don't match")
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

        result.left.value shouldBe IngestTypeCreateForExistingBag()
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

        result.left.value shouldBe IngestTypeUpdateForNewBag()
      }
    }
  }
}
