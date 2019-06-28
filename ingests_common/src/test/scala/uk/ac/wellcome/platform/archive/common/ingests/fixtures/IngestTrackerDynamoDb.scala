package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.fixtures.TestWith

import scala.util.Random

trait IngestTrackerDynamoDb extends LocalDynamoDb {
  private def createIngestTrackerTable(
    dynamoDbClient: AmazonDynamoDB): Table = {
    val tableName = Random.alphanumeric.take(10).mkString
    val tableIndex = Random.alphanumeric.take(10).mkString
    val table = Table(tableName, tableIndex)

    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("id")
          .withKeyType(KeyType.HASH))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("storageSpace")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("externalIdentifier")
            .withAttributeType("S")
        )
        .withGlobalSecondaryIndexes(
          new GlobalSecondaryIndex()
            .withIndexName(table.index)
            .withProjection(
              new Projection()
                .withProjectionType(ProjectionType.ALL)
            )
            .withKeySchema(
              new KeySchemaElement()
                .withAttributeName("externalIdentifier")
                .withKeyType(KeyType.HASH),
              new KeySchemaElement()
                .withAttributeName("storageSpace")
                .withKeyType(KeyType.RANGE)
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

  def withIngestTrackerTable[R](testWith: TestWith[Table, R]): R =
    withSpecifiedLocalDynamoDbTable(createIngestTrackerTable) { table =>
      testWith(table)
    }
}
