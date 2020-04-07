package uk.ac.wellcome.platform.archive.ingests

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Completed
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with IngestsFixtures
    with IngestGenerators
    with IntegrationPatience {

  describe("marking an ingest as Completed") {
    val ingest = createIngestWith(
      createdDate = Instant.now()
    )

    val ingestStatusUpdate =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Completed
      )

    val expectedIngest = ingest.copy(
      status = Completed,
      events = ingestStatusUpdate.events
    )

    val expectedCallbackNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = ingest.callback.get.uri,
      payload = expectedIngest
    )

    implicit val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    val callbackNotificationMessageSender = new MemoryMessageSender()

    it("reads messages from the queue") {
      withLocalSqsQueue { queue =>
        withIngestWorker(
          queue,
          ingestTracker,
          callbackNotificationMessageSender
        ) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            callbackNotificationMessageSender
              .getMessages[CallbackNotification] shouldBe Seq(
              expectedCallbackNotification
            )

            getMessages(queue) shouldBe empty
          }
        }
      }
    }

    it("updates the ingest tracker") {
      val storedIngest = ingestTracker.get(ingest.id).right.value.identifiedT
      storedIngest.status shouldBe Completed
    }

    it("records the events in the ingest tracker") {
      assertIngestRecordedRecentEvents(
        ingestStatusUpdate.id,
        ingestStatusUpdate.events.map { _.description }
      )
    }
  }
}
