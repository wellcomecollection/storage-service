package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.model._
import org.scanamo.{Scanamo, Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManagerDao,
  IngestVersionManagerDaoTestCases,
  VersionRecord
}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.util.Failure

class DynamoIngestVersionManagerDaoTest
    extends IngestVersionManagerDaoTestCases[Table]
    with IngestVersionManagerTable {
  override def withDao[R](initialRecords: Seq[VersionRecord])(
    testWith: TestWith[IngestVersionManagerDao, R])(
    implicit table: Table): R = {
    Scanamo.exec(dynamoDbClient)(
      ScanamoTable[DynamoEntry](table.name).putAll(initialRecords.map {
        DynamoEntry(_)
      }.toSet))

    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoDbClient,
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
          "Cannot do operations on a non-existent table")
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
          id = randomAlphanumeric(),
          ingestId = createIngestID,
          version = 1
        )

        Scanamo.put(dynamoDbClient)(table.name)(record)

        withDao(initialRecords = Seq.empty) { dao =>
          val result = dao.lookupExistingVersion(record.ingestId)

          result shouldBe a[Failure[_]]
          result.failed.get shouldBe a[RuntimeException]
          result.failed.get.getMessage should startWith(
            "Did not find exactly one row with ingest ID")
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

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table")
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
          "Cannot do operations on a non-existent table")
      }
    }
  }
}
