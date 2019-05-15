package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.collection.JavaConverters._
import scala.util.Success

class DynamoIngestVersionManagerDaoTest extends FunSpec with Matchers with LocalDynamoDb with ExternalIdentifierGenerators {
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
                  List("externalIdentifier", "ingestId", "version", "ingestDate").asJava)
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

  def withDao[R](table: Table)(testWith: TestWith[DynamoIngestVersionManagerDao, R]): R = {
    val dao = new DynamoIngestVersionManagerDao(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(dao)
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
        val record = VersionRecord(
          externalIdentifier = createExternalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.now,
          version = 3
        )

        Scanamo.put(dynamoDbClient)(table.name)(record)

        withDao(table) { dao =>
          dao.lookupExistingVersion(record.ingestId) shouldBe Success(Some(record))
        }
      }
    }
  }
}
