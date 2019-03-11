package uk.ac.wellcome.platform.archive.ingests.services

import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.ingests.fixtures.CallbackNotificationServiceFixture

class CallbackNotificationServiceTest
    extends FunSpec
    with ScalaFutures
    with CallbackNotificationServiceFixture
    with IngestGenerators {

  val sendsCallbackStatus = Table(
    ("ingest-status", "callback-status"),
    (Ingest.Failed, Callback.Pending),
    (Ingest.Completed, Callback.Pending)
  )

  it(
    "sends a notification if there's a pending callback and the ingest is complete") {
    forAll(sendsCallbackStatus) { (ingestStatus, callbackStatus) =>
      withLocalSnsTopic { topic =>
        withCallbackNotificationService(topic) { service =>
          val ingest = createIngestWith(
            status = ingestStatus,
            callback = Some(
              createCallbackWith(status = callbackStatus)
            )
          )

          val future = service.sendNotification(ingest)

          whenReady(future) { _ =>
            val expectedNotification = CallbackNotification(
              id = ingest.id,
              callbackUri = ingest.callback.get.uri,
              payload = ingest
            )

            assertSnsReceives(
              expectedMessages = List(expectedNotification),
              topic = topic
            )
          }
        }
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
      withLocalSnsTopic { topic =>
        withCallbackNotificationService(topic) { service =>
          val ingest = createIngestWith(
            status = ingestStatus,
            callback = Some(createCallbackWith(status = callbackStatus))
          )

          val future = service.sendNotification(ingest)

          // Sleep for half a second to be sure the message would have been
          // sent if it was going to.
          Thread.sleep(500)

          whenReady(future) { _ =>
            assertSnsReceivesNothing(topic)
          }
        }
      }
    }
  }

  it("doesn't send a notification if there's no callback information") {
    withLocalSnsTopic { topic =>
      withCallbackNotificationService(topic) { service =>
        val ingest = createIngestWith(
          callback = None
        )

        val future = service.sendNotification(ingest)

        // Sleep for half a second to be sure the message would have been
        // sent if it was going to.
        Thread.sleep(500)

        whenReady(future) { _ =>
          assertSnsReceivesNothing(topic)
        }
      }
    }
  }
}
