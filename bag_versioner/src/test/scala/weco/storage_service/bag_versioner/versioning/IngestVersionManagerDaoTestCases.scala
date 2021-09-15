package weco.storage_service.bag_versioner.versioning

import org.scalatest.{Assertion, EitherValues, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage.NoMaximaValueError

import scala.util.{Failure, Success}

trait IngestVersionManagerDaoTestCases[Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with TryValues
    with VersionRecordGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](initialRecords: Seq[VersionRecord])(
    testWith: TestWith[IngestVersionManagerDao, R]
  )(implicit context: Context): R

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

        records.foreach { record =>
          val storedRecord =
            dao
              .lookupExistingVersion(ingestId = record.ingestId)
              .success
              .value
              .get

          assertRecordsEqual(storedRecord, record)
        }

        val latestRecordA =
          dao
            .lookupLatestVersionFor(
              externalIdentifier = ExternalIdentifier("acorn"),
              space = storageSpaceA
            )
            .value

        assertRecordsEqual(latestRecordA, recordA3)

        val latestRecordB =
          dao
            .lookupLatestVersionFor(
              externalIdentifier = ExternalIdentifier("barley"),
              space = storageSpaceB
            )
            .value

        assertRecordsEqual(latestRecordB, recordB2)

        dao
          .lookupLatestVersionFor(
            externalIdentifier = ExternalIdentifier("chestnut"),
            space = createStorageSpace
          )
          .left
          .value shouldBe a[NoMaximaValueError]
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
        val storedRecord =
          dao2.lookupExistingVersion(record.ingestId).success.value.get

        assertRecordsEqual(storedRecord, record)

        val latestRecord =
          dao2
            .lookupLatestVersionFor(
              externalIdentifier = record.externalIdentifier,
              space = record.storageSpace
            )
            .value

        assertRecordsEqual(latestRecord, record)
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
          val storedRecord =
            dao.lookupExistingVersion(record.ingestId).success.value.get

          assertRecordsEqual(storedRecord, record)
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
          dao
            .lookupLatestVersionFor(
              externalIdentifier = createExternalIdentifier,
              space = createStorageSpace
            )
            .left
            .value shouldBe a[NoMaximaValueError]
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
          version = version
        )
      }

      withContext { implicit context =>
        withDao(initialRecords) { dao =>
          val latestRecord =
            dao
              .lookupLatestVersionFor(externalIdentifier, storageSpace)
              .value

          assertRecordsEqual(latestRecord, initialRecords.last)
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
          val storedRecord1 =
            dao
              .lookupLatestVersionFor(
                externalIdentifier = externalIdentifier,
                space = record1.storageSpace
              )
              .value

          assertRecordsEqual(record1, storedRecord1)

          val storedRecord2 =
            dao
              .lookupLatestVersionFor(
                externalIdentifier = externalIdentifier,
                space = record2.storageSpace
              )
              .value

          assertRecordsEqual(record2, storedRecord2)

          dao
            .lookupLatestVersionFor(
              externalIdentifier = externalIdentifier,
              space = createStorageSpace
            )
            .left
            .value shouldBe a[NoMaximaValueError]
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

  protected def assertRecordsEqual(
    r1: VersionRecord,
    r2: VersionRecord
  ): Assertion =
    r1 shouldBe r2
}
