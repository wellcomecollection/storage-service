package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.platform.archive.ingests.services.IngestsWorker
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.util.Success

trait IngestsFixtures
    extends LocalDynamoDb
    with ScalaFutures
    with IngestTrackerFixture
    with CallbackNotificationServiceFixture
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with OperationFixtures {

  def withIngestWorker[R](queue: Queue, table: Table, sender: MessageSender[String])(
    testWith: TestWith[IngestsWorker[String], R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          withIngestTracker(table) { ingestTracker =>
            withCallbackNotificationService(sender) {
              callbackNotificationService =>
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
    }

  def withIngest[R](ingestTracker: IngestTracker)(
    testWith: TestWith[Ingest, R]): R = {
    val createdIngest = createIngest

    ingestTracker.initialise(createdIngest) shouldBe a[Success[_]]

    testWith(createdIngest)
  }

  def withConfiguredApp[R](testWith: TestWith[(Queue, MemoryMessageSender, Table), R]): R = {
    withLocalSqsQueue { queue =>
      val messageSender = createMessageSender
      withIngestTrackerTable { table =>
        withIngestWorker(queue, table, messageSender) { _ =>
          testWith((queue, messageSender, table))
        }
      }
    }
  }
}
