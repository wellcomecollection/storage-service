package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.MonitoringClientFixture
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorker
}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

trait IngestsFixtures
    extends LocalDynamoDb
    with ScalaFutures
    with IngestTrackerFixture
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  def withIngestWorker[R](queue: Queue,
                          table: Table,
                          messageSender: MemoryMessageSender)(
    testWith: TestWith[IngestsWorker[String], R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          withIngestTracker(table) { ingestTracker =>
            val callbackNotificationService =
              new CallbackNotificationService(messageSender)

            val service = new IngestsWorker(
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

  def withConfiguredApp[R](
    testWith: TestWith[(Queue, MemoryMessageSender, Table), R]): R = {
    withLocalSqsQueue { queue =>
      val messageSender = new MemoryMessageSender()
      withIngestTrackerTable { table =>
        withIngestWorker(queue, table, messageSender) { _ =>
          testWith((queue, messageSender, table))
        }
      }
    }
  }
}
