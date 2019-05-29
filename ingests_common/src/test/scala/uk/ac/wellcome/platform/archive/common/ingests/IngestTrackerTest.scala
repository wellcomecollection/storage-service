package uk.ac.wellcome.platform.archive.common.ingests

import java.time.Instant

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{GetItemRequest, PutItemRequest, UpdateItemRequest}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.{FunSpec, TryValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IdConstraintError
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class IngestTrackerTest
    extends FunSpec
    with LocalDynamoDb
    with MockitoSugar
    with IngestTrackerFixture
    with IngestGenerators
    with ScalaFutures
    with TryValues {

  describe("create") {
    it("creates an ingest") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)

          val ingest = result.success.value
          assertIngestCreated(ingest, table)
        }
      }
    }

    it("creates only one ingest for a given id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val ingest = createIngest

          val monitors = List(ingest, ingest)

          val result = Future.sequence(monitors.map( i =>
            Future.fromTry { ingestTracker.initialise(i) }
          ))
          whenReady(result.failed) { failedException =>
            failedException shouldBe a[IdConstraintError]
            failedException.getMessage should include(
              s"There is already a ingest tracker with id:${ingest.id}")

            assertIngestCreated(ingest, table)
          }
        }
      }
    }

    it("fails if create a record if creation fails") {
      withIngestTrackerTable { table =>
        val mockDynamoDbClient = mock[AmazonDynamoDB]
        val expectedException = new RuntimeException("root cause")
        when(mockDynamoDbClient.putItem(any[PutItemRequest]))
          .thenThrow(expectedException)

        withIngestTracker(table, dynamoDbClient = mockDynamoDbClient) {
          ingestTracker =>
            val ingest = createIngest

            val result = ingestTracker.initialise(ingest)

            val failedException = result.failed.get
            failedException shouldBe expectedException
        }
      }
    }
  }

  describe("read") {
    it("retrieves ingest by id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val initResult = ingestTracker.initialise(createIngest)
          val ingest = initResult.success.value
          assertIngestCreated(ingest, table)

          val getResult = ingestTracker.get(ingest.id).success.value
          getResult shouldBe Some(ingest)
        }
      }
    }

    it("returns None when no ingest matches id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val getResult = ingestTracker.get(createIngestID).success.value
          getResult shouldBe None
        }
      }
    }

    it("throws when it encounters an error") {
      withIngestTrackerTable { table =>
        val mockDynamoDbClient = mock[AmazonDynamoDB]
        val expectedException = new RuntimeException("root cause")
        when(mockDynamoDbClient.getItem(any[GetItemRequest]))
          .thenThrow(expectedException)

        withIngestTracker(table, dynamoDbClient = mockDynamoDbClient) {
          ingestTracker =>
            val getResult = ingestTracker.get(createIngestID)
            getResult.failed.get shouldBe expectedException
        }
      }
    }
  }

  describe("update") {
    it("sets the bag id to a ingest with none") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val bagId = createBagId

          val ingestUpdate = IngestStatusUpdate(
            ingest.id,
            Ingest.Processing,
            Some(bagId)
          )

          ingestTracker.update(ingestUpdate)

          val storedIngest = getStoredIngest(ingest, table)

          assertRecent(storedIngest.createdDate)
          assertRecent(storedIngest.lastModifiedDate)
          storedIngest.events.map(_.description) should contain theSameElementsAs ingestUpdate.events
            .map(_.description)
          storedIngest.events.foreach(event =>
            assertRecent(event.createdDate))

          storedIngest.bag shouldBe ingestUpdate.affectedBag
        }
      }
    }

    it("adds a single event to a monitor with no events") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val ingestUpdate = IngestEventUpdate(
            id = ingest.id,
            events = List(createIngestEvent)
          )

          ingestTracker.update(ingestUpdate)

          assertIngestCreated(ingest, table)

          assertIngestRecordedRecentEvents(
            id = ingestUpdate.id,
            expectedEventDescriptions = ingestUpdate.events.map(_.description),
            table = table
          )
        }
      }
    }

    it("adds a status update to a monitor with no events") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val someBagId = Some(createBagId)
          val ingestUpdate = IngestStatusUpdate(
            ingest.id,
            Ingest.Completed,
            affectedBag = someBagId,
            List(createIngestEvent)
          )

          ingestTracker.update(ingestUpdate)

          val actualIngest = assertIngestCreated(ingest, table)

          actualIngest.status shouldBe Ingest.Completed
          actualIngest.bag shouldBe someBagId

          assertIngestRecordedRecentEvents(
            id = ingestUpdate.id,
            expectedEventDescriptions = ingestUpdate.events.map {
              _.description
            },
            table = table
          )
        }
      }
    }

    it("adds a callback status update to a monitor with no events") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val ingestUpdate = IngestCallbackStatusUpdate(
            id = ingest.id,
            callbackStatus = Callback.Succeeded,
            events = List(createIngestEvent)
          )

          ingestTracker.update(ingestUpdate)

          val actualIngest = assertIngestCreated(ingest, table)

          actualIngest.callback shouldBe defined
          actualIngest.callback.get.status shouldBe Callback.Succeeded

          assertIngestRecordedRecentEvents(
            id = ingestUpdate.id,
            expectedEventDescriptions = ingestUpdate.events.map {
              _.description
            },
            table = table
          )
        }
      }
    }

    it("adds an update with multiple events") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val ingestUpdate = IngestEventUpdate(
            ingest.id,
            List(createIngestEvent, createIngestEvent)
          )

          ingestTracker.update(ingestUpdate)

          assertIngestCreated(ingest, table)

          assertIngestRecordedRecentEvents(
            ingestUpdate.id,
            ingestUpdate.events.map(_.description),
            table)
        }
      }
    }

    it("adds multiple events to a monitor") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)
          val ingest = result.success.value

          val updates = List(
            createIngestEventUpdateWith(ingest.id),
            createIngestEventUpdateWith(ingest.id)
          )

          updates.foreach(ingestTracker.update(_))

          assertIngestCreated(ingest, table)

          assertIngestRecordedRecentEvents(
            ingest.id,
            updates.flatMap(_.events.map(_.description)),
            table)
        }
      }
    }

    it("throws if put to dynamo fails when adding an event") {
      withIngestTrackerTable { table =>
        val mockDynamoDbClient = mock[AmazonDynamoDB]

        val expectedException = new RuntimeException("root cause")
        when(mockDynamoDbClient.updateItem(any[UpdateItemRequest]))
          .thenThrow(expectedException)

        withIngestTracker(table, dynamoDbClient = mockDynamoDbClient) {
          ingestTracker =>
            val update = createIngestEventUpdate

            val result = Try(ingestTracker.update(update))

            val failedException = result.failed.get
            failedException shouldBe a[RuntimeException]
            failedException shouldBe expectedException
        }
      }
    }
  }

  describe("find ingest by BagId") {
    it(
      "query for multiple Ingests for a same bag are returned in order of createdDate") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val beforeTime = Instant.parse("2018-12-01T11:50:00.00Z")
          val time = Instant.parse("2018-12-01T12:00:00.00Z")
          val afterTime = Instant.parse("2018-12-01T12:10:00.00Z")

          val resultA = ingestTracker.initialise(createIngestWith(createdDate = beforeTime))
          val ingestA = resultA.success.value

          val resultB = ingestTracker.initialise(createIngestWith(createdDate = time))
          val ingestB = resultB.success.value

          val resultC = ingestTracker.initialise(createIngestWith(createdDate = afterTime))
          val ingestC = resultC.success.value

          val bagId = createBagId

          val ingestAUpdate =
            createIngestUpdateWith(ingestA.id, bagId)
          ingestTracker.update(ingestAUpdate)
          val ingestBUpdate =
            createIngestUpdateWith(ingestB.id, bagId)
          ingestTracker.update(ingestBUpdate)
          val ingestCUpdate =
            createIngestUpdateWith(ingestC.id, bagId)
          ingestTracker.update(ingestCUpdate)

          val bagIngests = ingestTracker.findByBagId(bagId)

          bagIngests shouldBe List(
            BagIngest(
              id = ingestC.id,
              bagIdIndex = bagId.toString,
              createdDate = afterTime
            ),
            BagIngest(
              id = ingestB.id,
              bagIdIndex = bagId.toString,
              createdDate = time
            ),
            BagIngest(
              id = ingestA.id,
              bagIdIndex = bagId.toString,
              createdDate = beforeTime
            )
          ).map { Right(_) }
        }
      }
    }

    it("only returns the most recent 30 ingest entries") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val start = Instant.parse("2018-12-01T12:00:00.00Z")
          val eventualIngests: Seq[Try[Ingest]] =
            for (i <- 0 to 33)
              yield
                ingestTracker.initialise(
                  createIngestWith(createdDate = start.plusSeconds(i)))

          val bagId = createBagId

          eventualIngests.map(eventualIngest =>
            eventualIngest.map { ingest =>
              val ingestUpdate =
                createIngestUpdateWith(ingest.id, bagId)
              ingestTracker.update(ingestUpdate)
          })

          eventually {
            val bagIngests = ingestTracker.findByBagId(bagId)

            bagIngests should have size 30
          }
        }
      }
    }
  }

  private def createIngestUpdateWith(id: IngestID, bagId: BagId): IngestUpdate =
    createIngestStatusUpdateWith(
      id = id,
      status = Ingest.Processing,
      maybeBag = Some(bagId)
    )
}
