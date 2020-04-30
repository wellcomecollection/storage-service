package uk.ac.wellcome.platform.archive.ingests.services

import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Callback,
  CallbackNotification,
  Ingest
}

import scala.util.Success

class CallbackNotificationServiceTest
    extends AnyFunSpec
    with Matchers
    with IngestGenerators {

  it("sends a notification if the ingest is complete and the callback pending") {
    val completedIngestStatus = Table(
      "ingest-status",
      Ingest.Succeeded,
      Ingest.Failed
    )

    forAll(completedIngestStatus) { ingestStatus: Ingest.Completed =>
      assertNotificationSent(
        ingestStatus = ingestStatus,
        callbackStatus = Callback.Pending
      )
    }
  }

  it("does not send a notification if the ingest is not complete") {
    val incompleteIngestStatus = Table(
      "ingest-status",
      Ingest.Processing,
      Ingest.Accepted
    )

    forAll(incompleteIngestStatus) { ingestStatus =>
      assertNothingSent(
        ingestStatus = ingestStatus,
        callbackStatus = Callback.Pending
      )
    }
  }

  it("doesn't send a notification if the callback has already completed") {
    val ingestStatusTable = Table(
      "ingest-status",
      Ingest.Processing,
      Ingest.Accepted,
      Ingest.Succeeded,
      Ingest.Failed
    )

    val completedCallbackStatusTable = Table(
      "callback-status",
      Callback.Failed,
      Callback.Succeeded
    )

    forAll(ingestStatusTable) { ingestStatus =>
      forAll(completedCallbackStatusTable) { callbackStatus =>
        assertNothingSent(
          ingestStatus = ingestStatus,
          callbackStatus = callbackStatus
        )
      }
    }
  }

  it("doesn't send a notification if there's no callback information") {
    val messageSender = new MemoryMessageSender()
    val service = new CallbackNotificationService(messageSender)

    val ingest = createIngestWith(callback = None)

    service.sendNotification(ingest) shouldBe Success(())

    messageSender.messages shouldBe empty
  }

  private def assertNotificationSent(
    ingestStatus: Ingest.Status,
    callbackStatus: Callback.CallbackStatus
  ): Assertion = {
    debug(s"ingestStatus = $ingestStatus, callbackStatus = $callbackStatus")
    val messageSender = new MemoryMessageSender()
    val service = new CallbackNotificationService(messageSender)

    val ingest = createIngestWith(
      status = ingestStatus,
      callback = Some(createCallbackWith(status = callbackStatus))
    )

    service.sendNotification(ingest) shouldBe Success(())

    val expectedNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = ingest.callback.get.uri,
      payload = ingest
    )

    messageSender.getMessages[CallbackNotification] shouldBe Seq(
      expectedNotification
    )
  }

  private def assertNothingSent(
    ingestStatus: Ingest.Status,
    callbackStatus: Callback.CallbackStatus
  ): Assertion = {
    debug(s"ingestStatus = $ingestStatus, callbackStatus = $callbackStatus")
    val messageSender = new MemoryMessageSender()
    val service = new CallbackNotificationService(messageSender)

    val ingest = createIngestWith(
      status = ingestStatus,
      callback = Some(createCallbackWith(status = callbackStatus))
    )

    service.sendNotification(ingest) shouldBe Success(())

    messageSender.messages shouldBe empty
  }
}
