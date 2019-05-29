package uk.ac.wellcome.platform.archive.ingests.services

import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, CallbackNotification, Ingest}

import scala.util.Success

class CallbackNotificationServiceTest
    extends FunSpec
    with Matchers
    with IngestGenerators
    with TryValues {

  val sendsCallbackStatus = Table(
    ("ingest-status", "callback-status"),
    (Ingest.Failed, Callback.Pending),
    (Ingest.Completed, Callback.Pending)
  )

  it(
    "sends a notification if there's a pending callback and the ingest is complete") {
    forAll(sendsCallbackStatus) { (ingestStatus, callbackStatus) =>
      val messageSender = createMessageSender
      val service = new CallbackNotificationService(messageSender)

      val ingest = createIngestWith(
        status = ingestStatus,
        callback = Some(
          createCallbackWith(status = callbackStatus)
        )
      )

      service.sendNotification(ingest) shouldBe Success(())

      val expectedNotification = CallbackNotification(
        ingestId = ingest.id,
        callbackUri = ingest.callback.get.uri,
        payload = ingest
      )

      messageSender.getMessages[CallbackNotification] shouldBe Seq(expectedNotification)
    }
  }

  val doesNotSendCallbackStatus = Table(
    ("ingest-status", "callback-status"),
    (Ingest.Accepted, Callback.Pending),
    (Ingest.Accepted, Callback.Succeeded),
    (Ingest.Accepted, Callback.Failed),
    (Ingest.Processing, Callback.Pending),
    (Ingest.Processing, Callback.Succeeded),
    (Ingest.Processing, Callback.Failed),
    (Ingest.Failed, Callback.Succeeded),
    (Ingest.Failed, Callback.Failed),
    (Ingest.Completed, Callback.Succeeded),
    (Ingest.Completed, Callback.Failed)
  )

  it("doesn't send a notification if the callback has already been sent") {
    forAll(doesNotSendCallbackStatus) { (ingestStatus, callbackStatus) =>
      val messageSender = createMessageSender
      val service = new CallbackNotificationService(messageSender)

      val ingest = createIngestWith(
        status = ingestStatus,
        callback = Some(createCallbackWith(status = callbackStatus))
      )

      service.sendNotification(ingest) shouldBe Success(())

      messageSender.messages shouldBe empty
    }
  }

  it("doesn't send a notification if there's no callback information") {
    val messageSender = createMessageSender
    val service = new CallbackNotificationService(messageSender)

    val ingest = createIngestWith(
      callback = None
    )

    service.sendNotification(ingest) shouldBe Success(())

    messageSender.messages shouldBe empty
  }
}
