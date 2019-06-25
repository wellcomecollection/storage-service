package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManager, IngestVersionManagerTestCases}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

class DynamoIngestVersionManagerTest extends IngestVersionManagerTestCases[Table] with IngestVersionManagerTable {
  override def withManager[R](testWith: TestWith[IngestVersionManager, R])(implicit table: Table): R = {
    val dao = new DynamoIngestVersionManagerDao(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(
      new DynamoIngestVersionManager(dao)
    )
  }
}
