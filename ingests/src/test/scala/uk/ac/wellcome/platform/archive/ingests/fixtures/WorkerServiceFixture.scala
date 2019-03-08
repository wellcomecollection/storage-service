package uk.ac.wellcome.platform.archive.ingests.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.NotificationStreamFixture
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestUpdate
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.ingests.services.IngestsWorkerService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends CallbackNotificationServiceFixture
    with NotificationStreamFixture
    with IngestTrackerFixture {
  def withWorkerService[R](queue: Queue, table: Table, topic: Topic)(
    testWith: TestWith[IngestsWorkerService, R]): R =
    withNotificationStream[IngestUpdate, R](queue) { notificationStream =>
      withIngestTracker(table) { ingestTracker =>
        withCallbackNotificationService(topic) { callbackNotificationService =>
          val service = new IngestsWorkerService(
            notificationStream = notificationStream,
            ingestTracker = ingestTracker,
            callbackNotificationService = callbackNotificationService
          )

          service.run()

          testWith(service)
        }
      }
    }
}
