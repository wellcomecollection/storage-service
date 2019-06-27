package uk.ac.wellcome.platform.archive.common.versioning

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.storage.NoMaximaValueError

import scala.util.{Failure, Success}

trait IngestVersionManagerDaoTestCases[Context]
    extends FunSpec
    with Matchers
    with EitherValues
    with VersionRecordGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](initialRecords: Seq[VersionRecord])(
    testWith: TestWith[IngestVersionManagerDao, R])(
    implicit context: Context): R

  it("is internally consistent") {
    val storageSpaceA = createStorageSpace

    val recordA1 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      storageSpace = storageSpaceA,
      version = 1
    )

    val recordA2 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      storageSpace = storageSpaceA,
      version = 2
    )

    val recordA3 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      storageSpace = storageSpaceA,
      version = 3
    )

    val storageSpaceB = createStorageSpace

    val recordB1 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("barley"),
      ingestId = createIngestID,
      storageSpace = storageSpaceB,
      version = 1
    )

    val recordB2 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("barley"),
      ingestId = createIngestID,
      storageSpace = storageSpaceB,
      version = 2
    )

    val records = List(
      recordA1,
      recordA2,
      recordA3,
      recordB1,
      recordB2
    )

    withContext { implicit context =>
      withDao(initialRecords = Seq.empty) { dao =>
        records.foreach { r =>
          dao.storeNewVersion(r) shouldBe Success(())
        }

        records.foreach { r =>
          dao.lookupExistingVersion(ingestId = r.ingestId) shouldBe Success(
            Some(r))
        }

        dao.lookupLatestVersionFor(
          externalIdentifier = ExternalIdentifier("acorn"),
          storageSpace = storageSpaceA).right.value shouldBe recordA3

        dao.lookupLatestVersionFor(
          externalIdentifier = ExternalIdentifier("barley"),
          storageSpace = storageSpaceB).right.value shouldBe recordB2

        dao.lookupLatestVersionFor(
          externalIdentifier = ExternalIdentifier("chestnut"),
          storageSpace = createStorageSpace).left.value shouldBe a[NoMaximaValueError]
      }
    }
  }

  it("persists records") {
    val record = createVersionRecord

    withContext { implicit context =>
      withDao(initialRecords = Seq.empty) { dao1 =>
        dao1.storeNewVersion(record) shouldBe Success(())
      }

      withDao(initialRecords = Seq.empty) { dao2 =>
        dao2.lookupExistingVersion(record.ingestId) shouldBe Success(
          Some(record))

        dao2.lookupLatestVersionFor(
          externalIdentifier = record.externalIdentifier,
          storageSpace = record.storageSpace).right.value shouldBe record
      }
    }
  }

  describe("look up an existing version") {
    it("returns None if there are no existing records") {
      withContext { implicit context =>
        withDao(initialRecords = Seq.empty) { dao =>
          dao.lookupExistingVersion(createIngestID) shouldBe Success(None)
        }
      }
    }

    it("finds an existing version record for this ingest") {
      val record = createVersionRecord

      withContext { implicit context =>
        withDao(initialRecords = Seq(record)) { dao =>
          dao.lookupExistingVersion(record.ingestId) shouldBe Success(
            Some(record))
        }
      }
    }

    it("fails if the same ingest ID appears multiple times") {
      val ingestId = createIngestID

      val record1 = createVersionRecordWith(ingestId = ingestId, version = 1)
      val record2 = createVersionRecordWith(ingestId = ingestId, version = 2)

      withContext { implicit context =>
        withDao(initialRecords = Seq(record1, record2)) { dao =>
          dao.lookupExistingVersion(ingestId) shouldBe a[Failure[_]]
        }
      }
    }
  }

  describe("look up the latest version for") {
    it("returns None if there isn't one") {
      withContext { implicit context =>
        withDao(initialRecords = Seq.empty) { dao =>
          dao.lookupLatestVersionFor(
            externalIdentifier = createExternalIdentifier,
            storageSpace = createStorageSpace).left.value shouldBe a[NoMaximaValueError]
        }
      }
    }

    it("finds the latest version associated with an externalIdentifier") {
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      val initialRecords = (1 to 5).map { version =>
        createVersionRecordWith(
          externalIdentifier = externalIdentifier,
          storageSpace = storageSpace,
          version = version)
      }

      withContext { implicit context =>
        withDao(initialRecords) { dao =>
          dao.lookupLatestVersionFor(externalIdentifier, storageSpace).right.value shouldBe initialRecords(4)
        }
      }
    }

    it("only looks for the latest version in the given storage space") {
      val externalIdentifier = createExternalIdentifier

      val record1 = createVersionRecordWith(
        externalIdentifier = externalIdentifier
      )

      val record2 = createVersionRecordWith(
        externalIdentifier = externalIdentifier
      )

      withContext { implicit context =>
        withDao(initialRecords = Seq(record1, record2)) { dao =>
          dao.lookupLatestVersionFor(
            externalIdentifier = externalIdentifier,
            storageSpace = record1.storageSpace
          ).right.value shouldBe record1

          dao.lookupLatestVersionFor(
            externalIdentifier = externalIdentifier,
            storageSpace = record2.storageSpace
          ).right.value shouldBe record2

          dao.lookupLatestVersionFor(
            externalIdentifier = externalIdentifier,
            storageSpace = createStorageSpace
          ).left.value shouldBe a[NoMaximaValueError]
        }
      }
    }
  }

  describe("store a new version") {
    it("stores a record in the table") {
      withContext { implicit context =>
        withDao(initialRecords = Seq.empty) { dao =>
          dao.storeNewVersion(createVersionRecord) shouldBe Success(())
        }
      }
    }
  }
}
