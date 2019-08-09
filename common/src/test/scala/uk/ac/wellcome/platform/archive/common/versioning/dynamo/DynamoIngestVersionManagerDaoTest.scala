package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.temporal.ChronoUnit

import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.{Assertion, EitherValues}
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManagerDao,
  IngestVersionManagerDaoTestCases,
  VersionRecord
}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

import scala.util.Failure

class DynamoIngestVersionManagerDaoTest
    extends IngestVersionManagerDaoTestCases[Table]
    with IngestVersionManagerTable
    with EitherValues {
  override def withDao[R](initialRecords: Seq[VersionRecord])(
    testWith: TestWith[IngestVersionManagerDao, R]
  )(implicit table: Table): R = {
    scanamo.exec(
      ScanamoTable[DynamoVersionRecord](table.name).putAll(initialRecords.map {
        DynamoVersionRecord(_)
      }.toSet)
    )

    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoClient,
        dynamoConfig = createDynamoConfigWith(table)
      )
    )
  }

  describe("look up an existing version in DynamoDB") {
    it("fails if there's an error connecting to DynamoDB") {
      implicit val badTable: Table = Table("does-not-exist", "does-not-exist")

      withDao(initialRecords = Seq.empty) { dao =>
        val result = dao.lookupExistingVersion(createIngestID)

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table"
        )
      }
    }

    it("fails if the DynamoDB table format is wrong") {
      withLocalDynamoDbTable { implicit table =>
        case class BadRecord(
          id: String,
          ingestId: IngestID,
          version: Int
        )

        val record = BadRecord(
          id = randomAlphanumericWithLength(),
          ingestId = createIngestID,
          version = 1
        )

        scanamo.exec(ScanamoTable[BadRecord](table.name).put(record))

        withDao(initialRecords = Seq.empty) { dao =>
          val result = dao.lookupExistingVersion(record.ingestId)

          result shouldBe a[Failure[_]]
          result.failed.get shouldBe a[RuntimeException]
          result.failed.get.getMessage should startWith(
            "Did not find exactly one row with ingest ID"
          )
        }
      }
    }
  }

  describe("look up the latest version in DynamoDB") {
    it("fails if it cannot reach the table") {
      implicit val badTable: Table = Table("does-not-exist", "does-not-exist")

      withDao(initialRecords = Seq.empty) { dao =>
        val result = dao.lookupLatestVersionFor(
          externalIdentifier = createExternalIdentifier,
          storageSpace = createStorageSpace
        )

        result.left.value.e shouldBe a[ResourceNotFoundException]
        result.left.value.e.getMessage should startWith(
          "Cannot do operations on a non-existent table"
        )
      }
    }
  }

  describe("store a new version in DynamoDB") {
    it("fails if it cannot reach the table") {
      implicit val badTable: Table = Table("does-not-exist", "does-not-exist")

      withDao(initialRecords = Seq.empty) { dao =>
        val result = dao.storeNewVersion(createVersionRecord)

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table"
        )
      }
    }
  }

  override protected def assertRecordsEqual(r1: VersionRecord, r2: VersionRecord): Assertion = {
    // DynamoDB only serialises an Instant to the nearest second, but
    // an Instant can have millisecond precision.
    //
    // This means the Instant we send in may not be the Instant that
    // gets stored, e.g. 2001-01-01:01:01:01.000999Z gets returned as
    //                   2001-01-01:01:01:01.000Z
    //
    val adjusted1 = r1.copy(ingestDate = r1.ingestDate.truncatedTo(ChronoUnit.SECONDS))
    val adjusted2 = r2.copy(ingestDate = r2.ingestDate.truncatedTo(ChronoUnit.SECONDS))

    adjusted1 shouldBe adjusted2
  }
}
