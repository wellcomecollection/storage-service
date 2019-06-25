package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.{Scanamo, Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, IngestVersionManagerDaoTestCases, VersionRecord}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.collection.JavaConverters._
import scala.util.Failure

class DynamoIngestVersionManagerDaoTest extends IngestVersionManagerDaoTestCases[Table] with LocalDynamoDb {
  override def createTable(table: LocalDynamoDb.Table): LocalDynamoDb.Table = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("externalIdentifier")
          .withKeyType(KeyType.HASH))
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("version")
          .withKeyType(KeyType.RANGE))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("externalIdentifier")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("ingestId")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("version")
            .withAttributeType("N")
        )
        .withGlobalSecondaryIndexes(
          new GlobalSecondaryIndex()
            .withIndexName(table.index)
            .withProjection(
              new Projection()
                .withProjectionType(ProjectionType.INCLUDE)
                .withNonKeyAttributes(
                  List(
                    "externalIdentifier",
                    "ingestId",
                    "version",
                    "ingestDate").asJava)
            )
            .withKeySchema(
              new KeySchemaElement()
                .withAttributeName("ingestId")
                .withKeyType(KeyType.HASH)
            )
            .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(1L)
              .withWriteCapacityUnits(1L))
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
    )
    eventually {
      waitUntilActive(dynamoDbClient, table.name)
    }
    table
  }

  override def withContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }

  override def withDao[R](initialRecords: Seq[VersionRecord])(testWith: TestWith[IngestVersionManagerDao, R])(implicit table: Table): R = {
    Scanamo.exec(dynamoDbClient)(ScanamoTable[VersionRecord](table.name).putAll(initialRecords.toSet))

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
          externalIdentifier: ExternalIdentifier,
          ingestId: IngestID,
          version: Int
        )

        val record = BadRecord(
          externalIdentifier = createExternalIdentifier,
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
        val result = dao.lookupLatestVersionFor(createExternalIdentifier)

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
