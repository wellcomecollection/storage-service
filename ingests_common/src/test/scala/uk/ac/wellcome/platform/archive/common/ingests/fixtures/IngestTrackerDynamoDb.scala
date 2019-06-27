package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

import scala.collection.JavaConverters._
import scala.util.Random

trait IngestTrackerDynamoDb extends DynamoFixtures {
  private def createIngestTrackerTable(
    dynamoDbClient: AmazonDynamoDB): Table = {
    val tableName = Random.alphanumeric.take(10).mkString
    val tableIndex = Random.alphanumeric.take(10).mkString
    val table = Table(tableName, tableIndex)

    createTableFromRequest(
      table,
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
            .withAttributeName("space")
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
                .withAttributeName("space")
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
  }

  def withIngestTrackerTable[R](testWith: TestWith[Table, R]): R =
    withSpecifiedLocalDynamoDbTable(createIngestTrackerTable) { table =>
      testWith(table)
    }
}
