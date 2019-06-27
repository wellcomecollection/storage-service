package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.scanamo.auto._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{BetterIngestTracker, BetterIngestTrackerTestCases}
import uk.ac.wellcome.storage.{StoreReadError, StoreWriteError, UpdateWriteError, Version}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTrackerTest extends BetterIngestTrackerTestCases[VersionedStore[IngestID, Int, Ingest]] with DynamoFixtures {
  def createIngestTrackerTable(table: Table): Table =
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
            .withAttributeName("bagIdIndex")
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
                .withAttributeName("bagIdIndex")
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

  override def withStoreImpl[R](testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      val config = createDynamoConfigWith(table)

      testWith(
        new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
        )
      )
    }

  // TODO: This can be commonised
  override def withIngestTracker[R](initialIngests: Seq[Ingest])(testWith: TestWith[BetterIngestTracker, R])(
    implicit store: VersionedStore[IngestID, Int, Ingest]): R = {
    initialIngests.foreach { ingest =>
      store.init(ingest.id)(ingest)
    }

    testWith(new DynamoIngestTracker(store))
  }

  override def withBrokenInitStoreImpl[R](testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      val config = createDynamoConfigWith(table)

      testWith(
        new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
        ) {
          override def init(id: IngestID)(t: Ingest) = Left(StoreWriteError(new Throwable("BOOM!")))
        }
      )
    }

  override def withBrokenGetStoreImpl[R](testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      val config = createDynamoConfigWith(table)

      testWith(
        new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
        ) {
          override def getLatest(id: IngestID) = Left(StoreReadError(new Throwable("BOOM!")))
        }
      )
    }

  override def withBrokenUpdateStoreImpl[R](testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      val config = createDynamoConfigWith(table)

      testWith(
        new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
        ) {
          override def update(id: IngestID)(f: Ingest => Ingest) = Left(UpdateWriteError(new Throwable("BOOM!")))
        }
      )
    }
}
