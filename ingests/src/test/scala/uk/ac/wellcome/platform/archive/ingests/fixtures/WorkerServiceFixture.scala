package uk.ac.wellcome.platform.archive.ingests.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.NotificationStreamFixture
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.progress.fixtures.ProgressTrackerFixture
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.platform.archive.ingests.services.IngestsWorkerService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends CallbackNotificationServiceFixture
    with NotificationStreamFixture
    with ProgressTrackerFixture {
  def withWorkerService[R](queue: Queue, table: Table, topic: Topic)(
    testWith: TestWith[IngestsWorkerService, R]): R =
    withNotificationStream[ProgressUpdate, R](queue) { notificationStream =>
      withProgressTracker(table) { progressTracker =>
        withCallbackNotificationService(topic) { callbackNotificationService =>
          val service = new IngestsWorkerService(
            notificationStream = notificationStream,
            progressTracker = progressTracker,
            callbackNotificationService = callbackNotificationService
          )

          service.run()

          testWith(service)
        }
      }
    }
}
