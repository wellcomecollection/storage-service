package uk.ac.wellcome.platform.archive.ingests.services

import org.scalatest.FunSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, CallbackNotification, Ingest}
import uk.ac.wellcome.platform.archive.ingests.fixtures.CallbackNotificationServiceFixture

import scala.util.Success

class CallbackNotificationServiceTest
    extends FunSpec
    with CallbackNotificationServiceFixture
    with IngestGenerators
    with OperationFixtures {

  val sendsCallbackStatus = Table(
    ("ingest-status", "callback-status"),
    (Ingest.Failed, Callback.Pending),
    (Ingest.Completed, Callback.Pending)
  )

  it(
    "sends a notification if there's a pending callback and the ingest is complete") {
    forAll(sendsCallbackStatus) { (ingestStatus, callbackStatus) =>
      val messageSender = createMessageSender
      withCallbackNotificationService(messageSender) { service =>
        val ingest = createIngestWith(
          status = ingestStatus,
          callback = Some(
            createCallbackWith(status = callbackStatus)
          )
        )

        service.sendNotification(ingest) shouldBe a[Success[_]]

        val expectedNotification = CallbackNotification(
          ingestId = ingest.id,
          callbackUri = ingest.callback.get.uri,
          payload = ingest
        )

        messageSender.getMessages[CallbackNotification]()  shouldBe Seq(expectedNotification)
      }
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
      withCallbackNotificationService(messageSender) { service =>
        val ingest = createIngestWith(
          status = ingestStatus,
          callback = Some(createCallbackWith(status = callbackStatus))
        )

        service.sendNotification(ingest) shouldBe a[Success[_]]

        messageSender.messages shouldBe empty
      }
    }
  }

  it("doesn't send a notification if there's no callback information") {
    val messageSender = createMessageSender
    withCallbackNotificationService(messageSender) { service =>
      val ingest = createIngestWith(
        callback = None
      )

      service.sendNotification(ingest) shouldBe a[Success[_]]

      messageSender.messages shouldBe empty
    }
  }
}
