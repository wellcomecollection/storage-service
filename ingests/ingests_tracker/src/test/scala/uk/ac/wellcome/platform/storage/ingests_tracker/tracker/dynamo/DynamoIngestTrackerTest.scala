package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.dynamo

import java.time.temporal.ChronoUnit

import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestEvent,
  IngestID
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.{
  IngestTracker,
  IngestTrackerTestCases
}
import uk.ac.wellcome.storage.{
  ReadError,
  StoreReadError,
  StoreWriteError,
  Version
}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.{Table => DynamoTable}
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DynamoIngestTrackerTest
    extends IngestTrackerTestCases[DynamoTable]
    with DynamoFixtures
    with RandomThings
    with ScalaFutures {
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
            override def max(hashKey: IngestID): Either[ReadError, Int] =
              Left(StoreReadError(new Throwable("BOOM!")))

            override def get(id: Version[IngestID, Int]): ReadEither =
              Left(StoreReadError(new Throwable("BOOM!")))

            override def put(
              id: Version[IngestID, Int]
            )(t: Ingest): WriteEither =
              Left(StoreWriteError(new Throwable("BOOM!")))
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
            override def max(hashKey: IngestID): Either[ReadError, Int] =
              Left(StoreReadError(new Throwable("BOOM!")))
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

  // This test is added temporarily to reproduce https://github.com/wellcomecollection/platform/issues/4781
  // What does the Dynamo ingest tracker do if you fire in lots of updates
  // for the same ingest in quick succession?
  it("reproduces the error") {
    val ingest = createIngest

    val updates = (1 to 10)
      .map { _ => createIngestEventUpdateWith(id = ingest.id) }

    withContext { implicit context =>
      withIngestTracker(initialIngests = Seq(ingest)) { tracker =>
        val futures = Future.sequence(
          updates.map { u => Future(tracker.update(u)) }
        )

        whenReady(futures) { r =>
          r.foreach { println(_) }
        }
      }
    }
  }
}
