package uk.ac.wellcome.platform.archive.progress_async

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.Progress.Completed
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.platform.archive.progress_async.fixtures.{IngestsFixture => ProgressFixture}

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with ProgressFixture
    with IntegrationPatience {

  it("updates an existing progress status to Completed") {
    withConfiguredApp {
      case (queue, topic, table) => {
        withProgressTracker(table) { monitor =>
          withProgress(monitor) { progress =>
            val someBagId = Some(createBagId)
            val progressStatusUpdate =
              createProgressStatusUpdateWith(
                id = progress.id,
                status = Completed,
                maybeBag = someBagId)

            sendNotificationToSQS[ProgressUpdate](queue, progressStatusUpdate)

            eventually {
              val actualMessage =
                notificationMessage[CallbackNotification](topic)
              actualMessage.id shouldBe progress.id
              actualMessage.callbackUri shouldBe progress.callback.get.uri

              val expectedProgress = progress.copy(
                status = Completed,
                events = progressStatusUpdate.events,
                bag = someBagId
              )
              actualMessage.payload shouldBe expectedProgress

              assertProgressCreated(progress, table)

              assertProgressRecordedRecentEvents(
                progressStatusUpdate.id,
                progressStatusUpdate.events.map(_.description),
                table
              )
            }
          }
        }
      }
    }
  }
}
