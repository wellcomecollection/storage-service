package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorker
}

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsFixtures
    extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

  def withIngestWorker[R](
    queue: Queue = Queue(url = "queue://test", arn = "arn::queue"),
    ingestTracker: IngestTracker,
    callbackNotificationMessageSender: MemoryMessageSender,
    updatedIngestsMessageSender: MemoryMessageSender = new MemoryMessageSender()
  )(testWith: TestWith[IngestsWorker[String, String], R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          val callbackNotificationService =
            new CallbackNotificationService(callbackNotificationMessageSender)

          val service = new IngestsWorker(
            alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
            ingestTracker = ingestTracker,
            callbackNotificationService = callbackNotificationService,
            updatedIngestsMessageSender = updatedIngestsMessageSender,
            metricsNamespace = "ingests_monitor"
          )

          service.run()

          testWith(service)
        }
      }
    }
}
