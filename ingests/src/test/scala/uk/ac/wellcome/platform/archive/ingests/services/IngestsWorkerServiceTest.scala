package uk.ac.wellcome.platform.archive.ingests.services

import io.circe.Encoder
import org.scalatest.{FunSpec, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Successful
}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  Ingest
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{
  Processing,
  Succeeded
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestDoesNotExistError
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.ingests.fixtures.IngestsFixtures

import scala.util.{Failure, Try}

class IngestsWorkerServiceTest
    extends FunSpec
    with IngestGenerators
    with IngestsFixtures
    with IngestTrackerFixtures
    with TryValues {

  describe("marking an ingest as completed") {
    val ingest = createIngest

    val ingestStatusUpdate =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Succeeded
      )

    val expectedIngest = ingest.copy(
      status = Succeeded,
      events = ingestStatusUpdate.events
    )

    val callbackNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = ingest.callback.get.uri,
      payload = expectedIngest
    )

    val callbackNotificationMessageSender = new MemoryMessageSender()
    val updatedIngestsMessageSender = new MemoryMessageSender()

    implicit val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    it("processes the message") {
      withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) {
        _.processMessage(ingestStatusUpdate).success.value shouldBe a[
          Successful[_]
        ]
      }
    }

    it("sends a callback notification message") {
      callbackNotificationMessageSender
        .getMessages[CallbackNotification] shouldBe Seq(
        callbackNotification
      )
    }

    it("records the ingest update in the tracker") {
      assertIngestRecordedRecentEvents(
        id = ingestStatusUpdate.id,
        expectedEventDescriptions = ingestStatusUpdate.events.map {
          _.description
        }
      )
    }

    it("sends a message with the updated ingest") {
      updatedIngestsMessageSender.getMessages[Ingest] shouldBe Seq(expectedIngest)
    }
  }

  describe("adding multiple events to an ingest") {
    val ingest = createIngestWith(
      status = Processing
    )

    val ingestStatusUpdate1 =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Processing
      )

    val ingestStatusUpdate2 =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Processing
      )

    val expectedIngest = ingest.copy(
      events = ingestStatusUpdate1.events ++ ingestStatusUpdate2.events
    )

    val callbackNotificationMessageSender = new MemoryMessageSender()
    val updatedIngestsMessageSender = new MemoryMessageSender()

    implicit val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    it("processes both messages") {
      withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) { service =>
        service
          .processMessage(ingestStatusUpdate1)
          .success
          .value shouldBe a[Successful[_]]

        service
          .processMessage(ingestStatusUpdate2)
          .success
          .value shouldBe a[Successful[_]]
      }
    }

    it("adds the events to the ingest tracker") {
      ingestTracker
        .get(ingest.id)
        .right
        .value
        .identifiedT shouldBe expectedIngest
    }

    it("does not send a notification for ingests that are still processing") {
      callbackNotificationMessageSender.messages shouldBe empty
    }

    it("sends a message with the updated ingests") {
      val expectedIngest1 = ingest.copy(
        events = ingestStatusUpdate1.events
      )

      val expectedIngest2 = ingest.copy(
        events = ingestStatusUpdate1.events ++ ingestStatusUpdate2.events
      )

      updatedIngestsMessageSender.getMessages[Ingest] shouldBe Seq(expectedIngest1, expectedIngest2)
    }
  }

  describe("fails if the update refers to an ingest that isn't in the tracker") {
    val ingestUpdate = createIngestEventUpdate

    val callbackNotificationMessageSender = new MemoryMessageSender()
    val updatedIngestsMessageSender = new MemoryMessageSender()

    val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq.empty)

    it("does not process the message successfully") {
      val result = withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) {
        _.processMessage(ingestUpdate)
      }

      result.success.value shouldBe a[NonDeterministicFailure[_]]

      result.success.value
        .asInstanceOf[NonDeterministicFailure[_]]
        .failure shouldBe a[Throwable]
    }

    it("does not update the ingest tracker") {
      ingestTracker.get(ingestUpdate.id).left.value shouldBe a[IngestDoesNotExistError]
    }

    it("does not send any messages") {
      callbackNotificationMessageSender.messages shouldBe empty
      updatedIngestsMessageSender.messages shouldBe empty
    }
  }

  describe("fails if it cannot send a callback notification") {
    val ingest = createIngest

    val ingestStatusUpdate = createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

    val throwable = new Throwable("Callback go BOOM!")
    val callbackNotificationMessageSender = createBrokenSender(throwable)

    val updatedIngestsMessageSender = new MemoryMessageSender()

    val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    it("does not process the message successfully") {
      val result = withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) {
        _.processMessage(ingestStatusUpdate)
      }

      result.success.value shouldBe a[NonDeterministicFailure[_]]

      result.success.value
        .asInstanceOf[NonDeterministicFailure[_]]
        .failure shouldBe throwable
    }

    it("sends the updated ingest body") {
      updatedIngestsMessageSender.messages should not be empty
    }
  }

  describe("fails if it cannot send the updated ingest") {
    val ingest = createIngest

    val ingestStatusUpdate = createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

    val callbackNotificationMessageSender = new MemoryMessageSender()

    val throwable = new Throwable("Updated ingests topic go BOOM!")
    val updatedIngestsMessageSender = createBrokenSender(throwable)

    val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    it("does not process the message successfully") {
      val result = withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) {
        _.processMessage(ingestStatusUpdate)
      }

      result.success.value shouldBe a[NonDeterministicFailure[_]]

      result.success.value
        .asInstanceOf[NonDeterministicFailure[_]]
        .failure shouldBe throwable
    }

    it("sends the callback notification") {
      callbackNotificationMessageSender.messages should not be empty
    }
  }

  describe("fails if it cannot send any messages") {
    val ingest = createIngest

    val ingestStatusUpdate = createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

    val callbackNotificationMessageSender = createBrokenSender()
    val updatedIngestsMessageSender = createBrokenSender()

    val ingestTracker: MemoryIngestTracker =
      createMemoryIngestTrackerWith(initialIngests = Seq(ingest))

    it("does not process the message successfully") {
      val result = withIngestWorker(
        ingestTracker = ingestTracker,
        callbackNotificationMessageSender = callbackNotificationMessageSender,
        updatedIngestsMessageSender = updatedIngestsMessageSender
      ) {
        _.processMessage(ingestStatusUpdate)
      }

      result.success.value shouldBe a[NonDeterministicFailure[_]]

      val err = result.success.value
        .asInstanceOf[NonDeterministicFailure[_]]
        .failure

      err shouldBe a[Throwable]
      err.getMessage shouldBe "Both of the ongoing messages failed to send correctly!"
    }

    it("doesn't send any messages") {
      callbackNotificationMessageSender.messages shouldBe empty
      updatedIngestsMessageSender.messages shouldBe empty
    }
  }
  
  private def createBrokenSender(throwable: Throwable = new Throwable("BOOM!")): MemoryMessageSender =
    new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(throwable)
    }
}
