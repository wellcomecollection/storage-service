package uk.ac.wellcome.platform.storage.ingests_worker.fixtures

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.IngestsTrackerApi
import uk.ac.wellcome.platform.storage.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorker

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsWorkerFixtures
    extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

  def withIngestWorker[R](
    queue: Queue = Queue(
      url = "queue://test",
      arn = "arn::queue"
    ),
    tracker: IngestTracker,
    callbackNotificationMessageSender: MemoryMessageSender,
    updatedIngestsMessageSender: MemoryMessageSender = new MemoryMessageSender()
  )(testWith: TestWith[IngestsWorker, R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer(actorSystem) { implicit materializer =>
          val api: IngestsTrackerApi[String, String] =
            new IngestsTrackerApi[String, String] {

              val callbackNotificationService = new CallbackNotificationService(
                callbackNotificationMessageSender
              )

              override val messagingService: MessagingService[String, String] =
                new MessagingService(
                  callbackNotificationService,
                  updatedIngestsMessageSender
                )

              override val ingestTracker: IngestTracker = tracker

              override implicit lazy protected val sys: ActorSystem =
                actorSystem
              override implicit lazy protected val mat: Materializer =
                materializer
            }

          api.run()

          val service = new IngestsWorker(
            workerConfig = createAlpakkaSQSWorkerConfig(queue),
            metricsNamespace = "ingests_worker",
            trackerHost = "http://localhost:8080"
          )

          service.run()

          testWith(service)
        }
      }
    }
}
