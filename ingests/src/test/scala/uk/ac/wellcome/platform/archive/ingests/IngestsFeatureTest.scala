package uk.ac.wellcome.platform.archive.ingests

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends AnyFunSpec
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
        status = Succeeded
      )

    val expectedIngest = ingest.copy(
      status = Succeeded,
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
    val updatedIngestsMessageSender = new MemoryMessageSender()

    it("reads messages from the queue") {
      // A timeout is explicit here as we were seeing errors
      // where the message got resent in CI.
      withLocalSqsQueueAndDlqAndTimeout(visibilityTimeout = 5) {
        case QueuePair(queue, _) =>
          withIngestWorker(
            queue = queue,
            ingestTracker = ingestTracker,
            callbackNotificationMessageSender =
              callbackNotificationMessageSender,
            updatedIngestsMessageSender = updatedIngestsMessageSender
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
      storedIngest.status shouldBe Succeeded
    }

    it("records the events in the ingest tracker") {
      assertIngestRecordedRecentEvents(
        ingestStatusUpdate.id,
        ingestStatusUpdate.events.map { _.description }
      )
    }

    it("sends a message with the updated ingest") {
      updatedIngestsMessageSender.getMessages[Ingest] shouldBe Seq(
        expectedIngest
      )
    }
  }
}
