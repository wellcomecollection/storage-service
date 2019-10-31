package uk.ac.wellcome.platform.storage.bag_versioner.versioning.dynamo

import com.amazonaws.services.dynamodbv2.model._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

trait IngestVersionManagerTable extends DynamoFixtures {
  override def createTable(table: Table): Table =
    createTableFromRequest(
      table,
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(
          new KeySchemaElement()
            .withAttributeName("id")
            .withKeyType(KeyType.HASH)
        )
        .withKeySchema(
          new KeySchemaElement()
            .withAttributeName("version")
            .withKeyType(KeyType.RANGE)
        )
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
            .withProvisionedThroughput(
              new ProvisionedThroughput()
                .withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L)
            )
        )
        .withProvisionedThroughput(
          new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)
        )
    )

  def withContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }
}
