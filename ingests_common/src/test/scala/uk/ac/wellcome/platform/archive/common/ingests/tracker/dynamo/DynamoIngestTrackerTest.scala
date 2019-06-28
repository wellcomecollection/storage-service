package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.model._
import org.scanamo.{Table => ScanamoTable}
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestTracker, IngestTrackerTestCases}
import uk.ac.wellcome.storage.{ReadError, StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.{Table => DynamoTable}
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTrackerTest extends IngestTrackerTestCases[DynamoTable] with DynamoFixtures with RandomThings {
  override def withContext[R](testWith: TestWith[DynamoTable, R]): R =
    withSpecifiedTable(createIngestTrackerTable) { table =>
      testWith(table)
    }

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(testWith: TestWith[IngestTracker, R])(implicit table: DynamoTable): R = {
    initialIngests.map { ingest =>
      val entry = DynamoHashEntry(ingest.id, version = 0, payload = ingest)
      scanamo.exec(ScanamoTable[DynamoHashEntry[IngestID, Int, Ingest]](table.name).put(entry))
    }

    testWith(
      new DynamoIngestTracker(config = createDynamoConfigWith(table))
    )
  }

  override def createTable(table: DynamoTable): DynamoTable = createIngestTrackerTable(table)

  def createIngestTrackerTable(table: DynamoTable): DynamoTable =
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

  private def withBrokenPutTracker[R](testWith: TestWith[IngestTracker, R])(implicit table: DynamoTable): R = {
    val config = createDynamoConfigWith(table)

    testWith(
      new DynamoIngestTracker(config) {
        override val underlying = new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[IngestID, Int, Ingest](config) {
            override def put(id: Version[IngestID, Int])(t: Ingest): WriteEither =
              Left(StoreWriteError(new Throwable("BOOM!")))
          }
        )
      }
    )
  }

  override def withBrokenUnderlyingInitTracker[R](testWith: TestWith[IngestTracker, R])(implicit table: DynamoTable): R =
    withBrokenPutTracker { tracker =>
      testWith(tracker)
    }

  override def withBrokenUnderlyingGetTracker[R](testWith: TestWith[IngestTracker, R])(implicit table: DynamoTable): R = {
    val config = createDynamoConfigWith(table)

    testWith(
      new DynamoIngestTracker(config) {
        override val underlying = new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[IngestID, Int, Ingest](config) {
            override def max(hashKey: IngestID): Either[ReadError, Int] =
              Left(StoreReadError(new Throwable("BOOM!")))
          }
        )
      }
    )
  }
  
  override def withBrokenUnderlyingUpdateTracker[R](testWith: TestWith[IngestTracker, R])(implicit table: DynamoTable): R =
    withBrokenPutTracker { tracker =>
      testWith(tracker)
    }
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
