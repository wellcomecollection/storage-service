package uk.ac.wellcome.platform.archive.common.versioning

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

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
    val recordA1 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      version = 1
    )

    val recordA2 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      version = 2
    )

    val recordA3 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("acorn"),
      ingestId = createIngestID,
      version = 3
    )

    val recordB1 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("barley"),
      ingestId = createIngestID,
      version = 1
    )

    val recordB2 = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("barley"),
      ingestId = createIngestID,
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

        dao.lookupLatestVersionFor(ExternalIdentifier("acorn")) shouldBe Success(
          Some(recordA3))
        dao.lookupLatestVersionFor(ExternalIdentifier("barley")) shouldBe Success(
          Some(recordB2))
        dao.lookupLatestVersionFor(ExternalIdentifier("chestnut")) shouldBe Success(
          None)
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

        dao2.lookupLatestVersionFor(record.externalIdentifier) shouldBe Success(
          Some(record))
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
          dao.lookupLatestVersionFor(createExternalIdentifier) shouldBe Success(
            None)
        }
      }
    }

    it("finds the latest version associated with an externalIdentifier") {
      val externalIdentifier = createExternalIdentifier

      val initialEntries = (1 to 5).map { version =>
        createVersionRecordWith(
          externalIdentifier = externalIdentifier,
          version = version)
      }

      withContext { implicit context =>
        withDao(initialEntries) { dao =>
          dao.lookupLatestVersionFor(externalIdentifier) shouldBe Success(
            Some(initialEntries(4)))
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
