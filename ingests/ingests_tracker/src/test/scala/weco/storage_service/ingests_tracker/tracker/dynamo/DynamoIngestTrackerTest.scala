package weco.storage_service.ingests_tracker.tracker.dynamo

import java.time.temporal.ChronoUnit
import org.scalatest.Assertion
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import weco.fixtures.TestWith
import weco.storage_service.ingests.models.{Ingest, IngestEvent, IngestID}
import weco.storage_service.ingests.models.IngestID._
import weco.storage_service.ingests_tracker.tracker.{
  IngestTracker,
  IngestTrackerTestCases
}
import weco.storage.{MaximaReadError, StoreReadError, StoreWriteError, Version}
import weco.storage.dynamo._
import weco.storage.fixtures.DynamoFixtures
import weco.storage.fixtures.DynamoFixtures.{Table => DynamoTable}
import weco.storage.store.VersionedStore
import weco.storage.store.dynamo.{
  ConsistencyMode,
  DynamoHashStore,
  StronglyConsistent
}

import scala.language.higherKinds

class DynamoIngestTrackerTest
    extends IngestTrackerTestCases[DynamoTable]
    with DynamoFixtures {
  override def withContext[R](
    testWith: TestWith[DynamoTable, R]
  ): R =
    withLocalDynamoDbTable { ingestTrackerTable =>
      testWith(ingestTrackerTable)
    }

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
    testWith: TestWith[IngestTracker, R]
  )(implicit ingestTrackerTable: DynamoTable): R = {
    val tracker = new DynamoIngestTracker(
      config = createDynamoConfigWith(ingestTrackerTable)
    )

    initialIngests.foreach { ingest =>
      tracker.init(ingest)
    }

    testWith(tracker)
  }

  override def createTable(table: DynamoTable): DynamoTable =
    createIngestTrackerTable(table)

  def createIngestTrackerTable(table: DynamoTable): DynamoTable =
    createTableWithHashKey(
      table,
      keyName = "id",
      keyType = ScalarAttributeType.S
    )

  private def withBrokenPutTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit ingestTrackerTable: DynamoTable): R = {
    val config = createDynamoConfigWith(ingestTrackerTable)

    testWith(
      new DynamoIngestTracker(config = config) {
        override val underlying = new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[IngestID, Int, Ingest](config) {
            override def max(hashKey: IngestID): MaxEither =
              Left(MaximaReadError(new Throwable("BOOM!")))

            override def get(id: Version[IngestID, Int]): ReadEither =
              Left(StoreReadError(new Throwable("BOOM!")))

            override def put(
              id: Version[IngestID, Int]
            )(t: Ingest): WriteEither =
              Left(StoreWriteError(new Throwable("BOOM!")))

            override implicit val consistencyMode: ConsistencyMode =
              StronglyConsistent
          }
        )
      }
    )
  }

  override def withBrokenUnderlyingInitTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit ingestTrackerTable: DynamoTable): R =
    withBrokenPutTracker { tracker =>
      testWith(tracker)
    }

  override def withBrokenUnderlyingGetTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit ingestTrackerTable: DynamoTable): R = {
    val config = createDynamoConfigWith(ingestTrackerTable)

    testWith(
      new DynamoIngestTracker(config = config) {
        override val underlying = new VersionedStore[IngestID, Int, Ingest](
          new DynamoHashStore[IngestID, Int, Ingest](config) {
            override def max(hashKey: IngestID): MaxEither =
              Left(MaximaReadError(new Throwable("BOOM!")))
          }
        )
      }
    )
  }

  override def withBrokenUnderlyingUpdateTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit ingestTrackerTable: DynamoTable): R =
    withBrokenPutTracker { tracker =>
      testWith(tracker)
    }

  // TODO: Add tests for handling DynamoDB errors

  override protected def assertIngestsEqual(
    ingest1: Ingest,
    ingest2: Ingest
  ): Assertion = {
    // DynamoDB only serialises an Instant to the nearest second, but
    // an Instant can have millisecond precision.
    //
    // This means the Instant we send in may not be the Instant that
    // gets stored, e.g. 2001-01-01:01:01:01.000999Z gets returned as
    //                   2001-01-01:01:01:01.000Z
    //
    val adjusted1 = ingest1.copy(
      createdDate = ingest1.createdDate.truncatedTo(ChronoUnit.SECONDS)
    )
    val adjusted2 = ingest2.copy(
      createdDate = ingest2.createdDate.truncatedTo(ChronoUnit.SECONDS)
    )

    adjusted1 shouldBe adjusted2
  }

  override protected def assertIngestEventsEqual(
    event1: IngestEvent,
    event2: IngestEvent
  ): Assertion = {
    val adjusted1 = event1.copy(
      createdDate = event1.createdDate.truncatedTo(ChronoUnit.SECONDS)
    )
    val adjusted2 = event2.copy(
      createdDate = event2.createdDate.truncatedTo(ChronoUnit.SECONDS)
    )

    adjusted1 shouldBe adjusted2
  }
}
