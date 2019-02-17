package uk.ac.wellcome.platform.archive.progress_async.services

import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.{Callback, Progress}

import scala.concurrent.ExecutionContext.Implicits.global

class CallbackNotificationServiceTest extends FunSpec with ScalaFutures with ProgressGenerators with SNS {

  val sendsCallbackStatus = Table(
    ("progress-status", "callback-status"),
    (Progress.Failed, Callback.Pending),
    (Progress.Completed, Callback.Pending)
  )

  it("sends a notification if there's a pending callback and the progress is complete") {
    forAll(sendsCallbackStatus) { (progressStatus, callbackStatus) =>
      withLocalSnsTopic { topic =>
        withCallbackNotificationService(topic) { service =>
          val progress = createProgressWith(
            status = progressStatus,
            callback = Some(createCallbackWith(status = callbackStatus))
          )

          val future = service.sendNotification(progress)

          whenReady(future) { _ =>
            val expectedNotification = CallbackNotification(
              id = progress.id,
              callbackUri = progress.callback.get.uri,
              payload = progress
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
    ("progress-status", "callback-status"),
    (Progress.Accepted, Callback.Pending),
    (Progress.Accepted, Callback.Succeeded),
    (Progress.Accepted, Callback.Failed),
    (Progress.Processing, Callback.Pending),
    (Progress.Processing, Callback.Succeeded),
    (Progress.Processing, Callback.Failed),
    (Progress.Failed, Callback.Succeeded),
    (Progress.Failed, Callback.Failed),
    (Progress.Completed, Callback.Succeeded),
    (Progress.Completed, Callback.Failed)
  )

  it("doesn't send a notification if the callback has already been sent") {
    forAll(doesNotSendCallbackStatus) { (progressStatus, callbackStatus) =>
      withLocalSnsTopic { topic =>
        withCallbackNotificationService(topic) { service =>
          val progress = createProgressWith(
            status = progressStatus,
            callback = Some(createCallbackWith(status = callbackStatus))
          )

          val future = service.sendNotification(progress)

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

  private def withCallbackNotificationService[R](topic: Topic)(testWith: TestWith[CallbackNotificationService, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val service = new CallbackNotificationService(snsWriter)
      testWith(service)
    }
}
