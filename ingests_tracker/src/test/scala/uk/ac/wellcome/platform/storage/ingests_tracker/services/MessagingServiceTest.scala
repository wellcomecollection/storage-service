package uk.ac.wellcome.platform.storage.ingests_tracker.services

import io.circe.Encoder
import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{Accepted, Failed, Processing, Succeeded}
import uk.ac.wellcome.platform.archive.common.ingests.models.{CallbackNotification, Ingest}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.MessagingServiceFixtures

import scala.util.{Failure, Success, Try}

class MessagingServiceTest
    extends AnyFunSpec
    with IngestGenerators
    with MessagingServiceFixtures
    with IngestTrackerFixtures
    with TryValues {

  private def createBrokenSender(throwable: Throwable): MemoryMessageSender =
    new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(throwable)
    }

  describe("Ingest with status Succeeded") {
    val successfulIngest = createIngestWith(status = Succeeded)

    val callbackNotification = CallbackNotification(
      ingestId = successfulIngest.id,
      callbackUri = successfulIngest.callback.get.uri,
      payload = successfulIngest
    )
    
    withMessagingService() { case (callbackSender,ingestsSender,messagingService) =>
      val sent = messagingService.send(successfulIngest)

      it("returns a Success") {
        sent shouldBe a[Success[_]]
      }
      
      it("sends a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe Seq(callbackNotification)
      }

      it("sends on the Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe Seq(successfulIngest)
      }
    }
  }

  describe("Ingest with status Failed") {
    val failedIngest = createIngestWith(status = Failed)

    val callbackNotification = CallbackNotification(
      ingestId = failedIngest.id,
      callbackUri = failedIngest.callback.get.uri,
      payload = failedIngest
    )

    withMessagingService() { case (callbackSender,ingestsSender,messagingService) =>
      val sent = messagingService.send(failedIngest)

      it("returns a Success") {
        sent shouldBe a[Success[_]]
      }

      it("sends a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe Seq(callbackNotification)
      }

      it("sends on the Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe Seq(failedIngest)
      }
    }
  }

  describe("Ingest with status Accepted") {
    val acceptedIngest = createIngestWith(status = Accepted)

    withMessagingService() { case (callbackSender,ingestsSender,messagingService) =>
      val sent = messagingService.send(acceptedIngest)

      it("returns a Success") {
        sent shouldBe a[Success[_]]
      }

      it("does not send a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe empty
      }

      it("sends on the Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe Seq(acceptedIngest)
      }
    }
  }

  describe("Ingest with status Processing") {
    val processingIngest = createIngestWith(status = Processing)

    withMessagingService() { case (callbackSender,ingestsSender,messagingService) =>
      val sent = messagingService.send(processingIngest)

      it("returns a Success") {
        sent shouldBe a[Success[_]]
      }

      it("does not send a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe empty
      }

      it("sends on the Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe Seq(processingIngest)
      }
    }
  }

  describe("it cannot send a CallbackNotification") {
    val successfulIngest = createIngestWith(status = Succeeded)

    val throwable = new Throwable("BOOM!")
    val callbackSender = createBrokenSender(throwable)

    withMessagingService(
      callbackSender = callbackSender
    ) { case (_, ingestsSender, messagingService) =>
      val sent = messagingService.send(successfulIngest)

      it("returns a Failure") {
        sent.failed.get shouldBe throwable
      }

      it("sends an Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe Seq(successfulIngest)
      }

      it("does not send a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe empty
      }
    }
  }

  describe("it cannot send an Ingest") {
    val successfulIngest = createIngestWith(status = Succeeded)

    val throwable = new Throwable("BOOM!")
    val ingestsSender = createBrokenSender(throwable)

    withMessagingService(
      ingestsSender = ingestsSender
    ) { case (callbackSender, _, messagingService) =>
      val sent = messagingService.send(successfulIngest)

      it("returns a Failure") {
        sent.failed.get shouldBe throwable
      }

      it("does not send an Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe empty
      }

      val callbackNotification = CallbackNotification(
        ingestId = successfulIngest.id,
        callbackUri = successfulIngest.callback.get.uri,
        payload = successfulIngest
      )

      it("sends a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe Seq(callbackNotification)
      }
    }
  }

  describe("it cannot send any messages") {
    val successfulIngest = createIngestWith(status = Succeeded)

    val throwable = new Throwable("BOOM!")
    val ingestsSender = createBrokenSender(throwable)
    val callbackSender = createBrokenSender(throwable)

    withMessagingService(
      ingestsSender = ingestsSender,
      callbackSender = callbackSender
    ) { case (_, _, messagingService) =>
      val sent = messagingService.send(successfulIngest)

      it("returns a Failure") {
        sent.failed.get.getMessage shouldBe "Both of the ongoing messages failed to send correctly!"
      }

      it("does not send an Ingest") {
        ingestsSender.getMessages[Ingest] shouldBe empty
      }

      it("does not send a CallbackNotification") {
        callbackSender.getMessages[CallbackNotification] shouldBe empty
      }
    }
  }
}
