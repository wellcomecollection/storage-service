package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

trait IngestVersionManagerTable extends LocalDynamoDb {
  override def createTable(table: LocalDynamoDb.Table): LocalDynamoDb.Table = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("id")
          .withKeyType(KeyType.HASH))
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("version")
          .withKeyType(KeyType.RANGE))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
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
                .withProjectionType(ProjectionType.ALL)
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

  def withContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }
}
