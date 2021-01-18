package uk.ac.wellcome.platform.storage.bag_versioner.versioning.dynamo

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  CreateTableRequest,
  GlobalSecondaryIndex,
  KeySchemaElement,
  KeyType,
  Projection,
  ProjectionType,
  ProvisionedThroughput
}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

trait IngestVersionManagerTable extends DynamoFixtures {
  override def createTable(table: Table): Table =
    createTableFromRequest(
      table,
      CreateTableRequest.builder()
        .tableName(table.name)
        .keySchema(
          KeySchemaElement.builder()
            .attributeName("id")
            .keyType(KeyType.HASH)
            .build(),
          KeySchemaElement.builder()
            .attributeName("version")
            .keyType(KeyType.RANGE)
            .build()
        )
        .attributeDefinitions(
          AttributeDefinition.builder()
            .attributeName("id")
            .attributeType("S")
            .build(),
          AttributeDefinition.builder()
            .attributeName("ingestId")
            .attributeType("S")
            .build(),
          AttributeDefinition.builder()
            .attributeName("version")
            .attributeType("N")
            .build()
        )
        .globalSecondaryIndexes(
          GlobalSecondaryIndex.builder()
            .indexName(table.index)
            .projection(
              Projection.builder()
                .projectionType(ProjectionType.ALL)
                .build()
            )
            .keySchema(
              KeySchemaElement.builder()
                .attributeName("ingestId")
                .keyType(KeyType.HASH)
                .build()
            )
            .provisionedThroughput(
              ProvisionedThroughput.builder()
                .readCapacityUnits(1L)
                .writeCapacityUnits(1L)
                .build()
            )
            .build()
        )
    )

  def withContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }
}
