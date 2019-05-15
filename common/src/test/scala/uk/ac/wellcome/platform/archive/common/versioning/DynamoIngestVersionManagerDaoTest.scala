package uk.ac.wellcome.platform.archive.common.versioning

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class DynamoIngestVersionManagerDaoTest
    extends FunSpec
    with Matchers
    with LocalDynamoDb
    with VersionRecordGenerators {
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

  def withDao[R](table: Table)(
    testWith: TestWith[DynamoIngestVersionManagerDao, R]): R = {
    val dao = new DynamoIngestVersionManagerDao(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(dao)
  }

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

    withLocalDynamoDbTable { table =>
      withDao(table) { dao =>
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

  describe("lookupExistingVersion") {
    it("returns Success[None] if it can't find anything") {
      withLocalDynamoDbTable { table =>
        withDao(table) { dao =>
          dao.lookupExistingVersion(createIngestID) shouldBe Success(None)
        }
      }
    }

    it("returns Success[Some] if it finds an existing record") {
      withLocalDynamoDbTable { table =>
        val record = createVersionRecord

        Scanamo.put(dynamoDbClient)(table.name)(record)

        withDao(table) { dao =>
          dao.lookupExistingVersion(record.ingestId) shouldBe Success(
            Some(record))
        }
      }
    }

    it("fails if there's an error connecting to DynamoDB") {
      withDao(Table("does-not-exist", "does-not-exist")) { dao =>
        val result = dao.lookupExistingVersion(createIngestID)

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table")
      }
    }

    it("fails if the same ingest ID appears multiple times") {
      withLocalDynamoDbTable { table =>
        val ingestId = createIngestID

        val record1 = createVersionRecordWith(ingestId = ingestId, version = 1)
        val record2 = createVersionRecordWith(ingestId = ingestId, version = 2)

        Scanamo.put(dynamoDbClient)(table.name)(record1)
        Scanamo.put(dynamoDbClient)(table.name)(record2)

        withDao(table) { dao =>
          val result = dao.lookupExistingVersion(ingestId)

          result shouldBe a[Failure[_]]
          result.failed.get shouldBe a[RuntimeException]
          result.failed.get.getMessage should startWith(
            "Did not find exactly one row with ingest ID")
        }
      }
    }

    it("fails if the DynamoDB table format is wrong") {
      withLocalDynamoDbTable { table =>
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

        withDao(table) { dao =>
          val result = dao.lookupExistingVersion(record.ingestId)

          result shouldBe a[Failure[_]]
          result.failed.get shouldBe a[RuntimeException]
          result.failed.get.getMessage should startWith(
            "Did not find exactly one row with ingest ID")
        }
      }
    }
  }

  describe("lookupLatestVersionFor") {
    it("returns None if there isn't one") {
      withLocalDynamoDbTable { table =>
        withDao(table) { dao =>
          dao.lookupLatestVersionFor(createExternalIdentifier) shouldBe Success(
            None)
        }
      }
    }

    it("finds the latest version associated with an externalIdentifier") {
      withLocalDynamoDbTable { table =>
        val externalIdentifier = createExternalIdentifier

        val records = (1 to 5).map { version =>
          createVersionRecordWith(
            externalIdentifier = externalIdentifier,
            version = version)
        }

        records.foreach { r =>
          Scanamo.put(dynamoDbClient)(table.name)(r)
        }

        withDao(table) { dao =>
          dao.lookupLatestVersionFor(externalIdentifier) shouldBe Success(
            Some(records(4)))
        }
      }
    }

    it("fails if it cannot reach the table") {
      withDao(Table("does-not-exist", "does-not-exist")) { dao =>
        val result = dao.lookupLatestVersionFor(createExternalIdentifier)

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table")
      }
    }
  }

  describe("storeNewVersion") {
    it("stores a record in the table") {
      withLocalDynamoDbTable { table =>
        withDao(table) { dao =>
          val records = (1 to 3).map { version =>
            createVersionRecordWith(version = version)
          }

          records.foreach { r =>
            dao.storeNewVersion(r) shouldBe Success(())
          }

          val storedRecords =
            Scanamo
              .scan[VersionRecord](dynamoDbClient)(table.name)
              .map { _.right.get }

          storedRecords should contain theSameElementsAs records
        }
      }
    }

    it("fails if it cannot reach the table") {
      withDao(Table("does-not-exist", "does-not-exist")) { dao =>
        val result = dao.storeNewVersion(createVersionRecord)

        result shouldBe a[Failure[_]]
        result.failed.get shouldBe a[ResourceNotFoundException]
        result.failed.get.getMessage should startWith(
          "Cannot do operations on a non-existent table")
      }
    }
  }
}
