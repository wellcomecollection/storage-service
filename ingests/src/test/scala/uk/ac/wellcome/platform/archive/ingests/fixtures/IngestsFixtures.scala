package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.MonitoringClientFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.ingests.services.{
  CallbackNotificationService,
  IngestsWorker
}

trait IngestsFixtures
    extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with IngestTrackerFixtures {

  def withIngestWorker[R](
    queue: Queue = Queue(url = "queue://test", arn = "arn::queue"),
    ingestTracker: IngestTracker,
    callbackNotificationMessageSender: MemoryMessageSender
  )(testWith: TestWith[IngestsWorker[String], R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          val callbackNotificationService =
            new CallbackNotificationService(callbackNotificationMessageSender)

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

  def withConfiguredApp[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[(Queue, MemoryMessageSender, MemoryIngestTracker), R]
  ): R = {
    withLocalSqsQueue { queue =>
      val messageSender = new MemoryMessageSender()
      withMemoryIngestTracker(initialIngests) { ingestTracker =>
        withIngestWorker(queue, ingestTracker, messageSender) { _ =>
          testWith((queue, messageSender, ingestTracker))
        }
      }
    }
  }
}
