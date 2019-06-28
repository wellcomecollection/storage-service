package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.model._
import org.scanamo.{Table => ScanamoTable}
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestTracker, IngestTrackerTestCases}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

class DynamoIngestTrackerTest extends IngestTrackerTestCases[Table] with DynamoFixtures {
  override def withContext[R](testWith: TestWith[Table, R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      testWith(table)
    }

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(testWith: TestWith[IngestTracker, R])(implicit table: Table): R = {
    initialIngests.map { ingest =>
      val entry = DynamoHashEntry(ingest.id, version = 0, payload = ingest)
      scanamo.exec(ScanamoTable[DynamoHashEntry[IngestID, Int, Ingest]](table.name).put(entry))
    }

    testWith(
      new DynamoIngestTracker(config = createDynamoConfigWith(table))
    )
  }

  override def withBrokenInitStoreImpl[R](testWith: TestWith[Table, R]): R = ???

  override def withBrokenGetStoreImpl[R](testWith: TestWith[Table, R]): R = ???

  override def withBrokenUpdateStoreImpl[R](testWith: TestWith[Table, R]): R = ???

  override def createTable(table: Table): Table = createIngestTrackerTable(table)

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

//  override def withStoreImpl[R](
//                                 testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
//    withSpecifiedTable(createIngestTrackerTable) { table =>
//      val config = createDynamoConfigWith(table)
//
//      testWith(
//        new VersionedStore[IngestID, Int, Ingest](
//          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
//        )
//      )
//    }

}

//
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
//import com.amazonaws.services.dynamodbv2.model._
//import org.scanamo.auto._
//import uk.ac.wellcome.fixtures.TestWith
//import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
//import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
//import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
//  IngestTracker,
//  IngestTrackerTestCases
//}
//import uk.ac.wellcome.storage.{
//  StoreReadError,
//  StoreWriteError,
//  UpdateWriteError,
//  Version
//}
//import uk.ac.wellcome.storage.fixtures.DynamoFixtures
//import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
//import uk.ac.wellcome.storage.store.VersionedStore
//import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore
//
//class DynamoIngestTrackerTest
//    extends IngestTrackerTestCases[VersionedStore[IngestID, Int, Ingest]]
//    with DynamoFixtures {
//
//  // TODO: This can be commonised
//  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
//    testWith: TestWith[IngestTracker, R])(
//    implicit store: VersionedStore[IngestID, Int, Ingest]): R = {
//    initialIngests.foreach { ingest =>
//      store.init(ingest.id)(ingest)
//    }
//
//    testWith(new DynamoIngestTracker(store))
//  }
//
//  override def withBrokenInitStoreImpl[R](
//    testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
//    withSpecifiedTable(createIngestTrackerTable) { table =>
//      val config = createDynamoConfigWith(table)
//
//      testWith(
//        new VersionedStore[IngestID, Int, Ingest](
//          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
//        ) {
//          override def init(id: IngestID)(t: Ingest) =
//            Left(StoreWriteError(new Throwable("BOOM!")))
//        }
//      )
//    }
//
//  override def withBrokenGetStoreImpl[R](
//    testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
//    withSpecifiedTable(createIngestTrackerTable) { table =>
//      val config = createDynamoConfigWith(table)
//
//      testWith(
//        new VersionedStore[IngestID, Int, Ingest](
//          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
//        ) {
//          override def getLatest(id: IngestID) =
//            Left(StoreReadError(new Throwable("BOOM!")))
//        }
//      )
//    }
//
//  override def withBrokenUpdateStoreImpl[R](
//    testWith: TestWith[VersionedStore[IngestID, Int, Ingest], R]): R =
//    withSpecifiedTable(createIngestTrackerTable) { table =>
//      val config = createDynamoConfigWith(table)
//
//      testWith(
//        new VersionedStore[IngestID, Int, Ingest](
//          new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
//        ) {
//          override def update(id: IngestID)(f: Ingest => Ingest) =
//            Left(UpdateWriteError(new Throwable("BOOM!")))
//        }
//      )
//    }
//
//  // TODO: Add tests for handling DynamoDB errors
//
//  // TODO: Add tests for finite bag listing
//}
