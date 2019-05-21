package uk.ac.wellcome.platform.archive.common.ingests

import java.time.Instant

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ConditionalCheckFailedException, GetItemRequest, PutItemRequest, UpdateItemRequest}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb

import scala.collection.immutable
import scala.util.{Failure, Success}

class DynamoIngestTrackerTest
    extends FunSpec
    with LocalDynamoDb
    with MockitoSugar
    with IngestTrackerFixture
    with IngestGenerators
    with ScalaFutures {

  describe("create") {
    it("creates an ingest") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val result = ingestTracker.initialise(createIngest)

          result shouldBe a[Success[_]]
          val ingest = result.get
          assertIngestCreated(ingest, table)
        }
      }
    }

    it("creates only one ingest for a given id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val ingest = createIngest

          ingestTracker.initialise(ingest) shouldBe a[Success[_]]

          val result = ingestTracker.initialise(ingest)

          result shouldBe a[Failure[_]]
          val err = result.failed.get

          err shouldBe a[ConditionalCheckFailedException]
          err.getMessage should startWith("The conditional request failed")

          assertIngestCreated(ingest, table)
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

            result shouldBe a[Failure[_]]
            val err = result.failed.get
            err shouldBe a[RuntimeException]
            err shouldBe expectedException
        }
      }
    }
  }

  describe("read") {
    it("retrieves ingest by id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val ingest = ingestTracker.initialise(createIngest).get
          assertIngestCreated(ingest, table)

          ingestTracker.get(ingest.id) shouldBe Success(Some(ingest))
        }
      }
    }

    it("returns None when no ingest matches id") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          ingestTracker.get(createIngestID) shouldBe Success(None)
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
            val result = ingestTracker.get(createIngestID)

            result shouldBe Failure(expectedException)
        }
      }
    }
  }

  describe("update") {
    it("sets the bag id to a ingest with none") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val ingest = ingestTracker.initialise(createIngest).get
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
          val ingest = ingestTracker.initialise(createIngest).get
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
          val ingest = ingestTracker.initialise(createIngest).get
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
          val ingest = ingestTracker.initialise(createIngest).get
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
          val ingest = ingestTracker.initialise(createIngest).get
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
          val ingest = ingestTracker.initialise(createIngest).get
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

            val result = ingestTracker.update(update)

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

          val ingestA = ingestTracker.initialise(createIngestWith(createdDate = beforeTime)).get
          val ingestB = ingestTracker.initialise(createIngestWith(createdDate = time)).get
          val ingestC = ingestTracker.initialise(createIngestWith(createdDate = afterTime)).get

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

          val bagIngests = ingestTracker.findByBagId(bagId).get

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
          )
        }
      }
    }

    it("only returns the most recent 30 ingest entries") {
      withIngestTrackerTable { table =>
        withIngestTracker(table) { ingestTracker =>
          val start = Instant.parse("2018-12-01T12:00:00.00Z")
          val eventualIngests: immutable.Seq[Ingest] = (0 to 33).map { i =>
            ingestTracker.initialise(
              createIngestWith(createdDate = start.plusSeconds(i))).get
          }

          val bagId = createBagId

          eventualIngests.map { ingest =>
              val ingestUpdate =
                createIngestUpdateWith(ingest.id, bagId)
              ingestTracker.update(ingestUpdate)
          }

          eventually {
            val bagIngests = ingestTracker.findByBagId(bagId).get

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
