package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import java.time.Instant

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.storage.bag_versioner.fixtures.VersionPickerFixtures

class VersionPickerTest
    extends AnyFunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with VersionPickerFixtures
    with StorageSpaceGenerators
    with EitherValues {

  it(
    "assigns version 1 if it hasn't seen this external ID/storage space before"
  ) {
    withVersionPicker { picker =>
      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.now(),
        storageSpace = createStorageSpace
      )

      result.right.value shouldBe BagVersion(1)
    }
  }

  it("always picks the same version for a given ingest ID") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace
      val ingestId = createIngestID

      // Pick an initial version
      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.ofEpochSecond(1),
        storageSpace = storageSpace
      )

      // Now assign some more versions with different
      // ingest IDs.
      (2 to 4).map { t =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(t),
          storageSpace = storageSpace
        )
      }

      // Now assign another version, this time with a fixed ingest ID.
      val result = picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestType = UpdateIngestType,
        ingestDate = Instant.ofEpochSecond(100),
        storageSpace = storageSpace
      )

      // If we keep asking for a version with the same ingest ID, we
      // get the same ones back each time
      (1 to 5).foreach { _ =>
        picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(100),
          storageSpace = storageSpace
        ) shouldBe result
      }
    }
  }

  it(
    "assigns independent versions for the same external ID in different storage spaces"
  ) {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier

      (1 to 5).map { _ =>
        picker
          .chooseVersion(
            externalIdentifier = externalIdentifier,
            ingestId = createIngestID,
            ingestType = CreateIngestType,
            ingestDate = Instant.now(),
            storageSpace = createStorageSpace
          )
          .right
          .value shouldBe BagVersion(1)
      }
    }
  }

  it("picks monotonically increasing versions for an external identifier") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      picker
        .chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = CreateIngestType,
          ingestDate = Instant.ofEpochSecond(1),
          storageSpace = storageSpace
        )
        .right
        .value shouldBe BagVersion(1)

      (2 to 5).map { t =>
        picker
          .chooseVersion(
            externalIdentifier = externalIdentifier,
            ingestId = createIngestID,
            ingestType = UpdateIngestType,
            ingestDate = Instant.ofEpochSecond(t),
            storageSpace = storageSpace
          )
          .right
          .value shouldBe BagVersion(t)
      }
    }
  }

  it("fails if the ingest date goes backwards") {
    withVersionPicker { picker =>
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.now(),
        storageSpace = storageSpace
      )

      (1 to 3).map { t =>
        val result = picker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestType = UpdateIngestType,
          ingestDate = Instant.ofEpochSecond(t),
          storageSpace = storageSpace
        )

        result.left.value shouldBe a[UnableToAssignVersion]

        val err: UnableToAssignVersion =
          result.left.value.asInstanceOf[UnableToAssignVersion]
        err.ingestVersionManagerError shouldBe a[NewerIngestAlreadyExists]
      }
    }
  }

  it("locks around the ingest ID, external identifier and storage space") {
    val lockDao = createLockDao

    withVersionPicker(lockDao) { picker =>
      val ingestId = createIngestID
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      picker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestType = CreateIngestType,
        ingestDate = Instant.now(),
        storageSpace = storageSpace
      )

      lockDao.getCurrentLocks shouldBe empty

    // TODO: Restore history on the MemoryLockDao
//      lockDao.history.map { _.id } should contain theSameElementsAs List(
//        s"ingest:$ingestId",
//        s"external:$storageSpace:$externalIdentifier"
//      )
    }
  }

  it("fails with FailedToGetLock if there is a LockFailure") {
    val lockDao = createBrokenLockDao

    withVersionPicker(lockDao) { picker =>
      val result: Either[VersionPickerError, BagVersion] = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.now(),
        storageSpace = createStorageSpace
      )

      result.left.value shouldBe a[FailedToGetLock]
    }
  }

  it("errors if there's an existing ingest with the wrong external identifier") {
    withVersionPicker { picker =>
      val ingestId = createIngestID

      picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = ingestId,
        ingestType = CreateIngestType,
        ingestDate = Instant.now(),
        storageSpace = createStorageSpace
      )

      val result = picker.chooseVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = ingestId,
        ingestType = UpdateIngestType,
        ingestDate = Instant.now(),
        storageSpace = createStorageSpace
      )

      result.left.value shouldBe a[UnableToAssignVersion]

      val err = result.left.value.asInstanceOf[UnableToAssignVersion]
      err.ingestVersionManagerError shouldBe a[ExternalIdentifiersMismatch]
    }
  }
}
