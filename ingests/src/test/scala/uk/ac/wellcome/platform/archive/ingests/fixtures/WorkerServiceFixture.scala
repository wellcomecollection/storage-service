package uk.ac.wellcome.platform.archive.ingests.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.MonitoringClientFixture
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.ingests.services.NewIngestsWorker
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends CallbackNotificationServiceFixture
    with IngestTrackerFixture
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {
  def withWorkerService[R](queue: Queue, table: Table, topic: Topic)(
    testWith: TestWith[NewIngestsWorker, R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          withIngestTracker(table) { ingestTracker =>
            withCallbackNotificationService(topic) { callbackNotificationService =>
              val service = new NewIngestsWorker(
                alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
                ingestTracker = ingestTracker,
                callbackNotificationService = callbackNotificationService
              )

              service.run()

              testWith(service)
            }
          }
        }
      }
    }
}
