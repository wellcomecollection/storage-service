package weco.storage_service.ingests_tracker.tracker.dynamo

import java.time.temporal.ChronoUnit
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import weco.fixtures.TestWith
import weco.storage_service.ingests.models.{
  Callback,
  CreateIngestType,
  Ingest,
  IngestEvent,
  IngestID,
  IngestType,
  SourceLocation
}
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
import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.ingests.models.Ingest.Status
import weco.storage_service.storage.models.StorageSpace

import java.time.Instant
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

  // DynamoDB only serialises an Instant to the nearest second, but
  // an Instant can have millisecond precision.
  //
  // This means the Instant we send in may not be the Instant that
  // gets stored, e.g. 2001-01-01:01:01:01.000999Z gets returned as
  //                   2001-01-01:01:01:01.000Z
  //
  override def createIngestWith(
    id: IngestID = createIngestID,
    ingestType: IngestType = CreateIngestType,
    sourceLocation: SourceLocation = createSourceLocation,
    callback: Option[Callback] = Some(createCallback()),
    space: StorageSpace = createStorageSpace,
    status: Status = Ingest.Accepted,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: Option[BagVersion] = maybeVersion,
    createdDate: Instant =
      Instant.now().plusSeconds(randomInt(from = 0, to = 30)),
    events: Seq[IngestEvent] = Seq.empty
  ): Ingest =
    Ingest(
      id = id,
      ingestType = ingestType,
      sourceLocation = sourceLocation,
      callback = callback,
      space = space,
      status = status,
      externalIdentifier = externalIdentifier,
      version = version,
      createdDate = createdDate.truncatedTo(ChronoUnit.SECONDS),
      events = events
    )

  override def createIngestEventWith(
    description: String = randomAlphanumeric(),
    createdDate: Instant =
      Instant.now().plusSeconds(randomInt(from = 0, to = 30))
  ): IngestEvent =
    IngestEvent(
      description = description,
      createdDate = createdDate.truncatedTo(ChronoUnit.SECONDS)
    )
}
