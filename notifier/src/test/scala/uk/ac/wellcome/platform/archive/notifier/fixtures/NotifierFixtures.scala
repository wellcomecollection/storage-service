package uk.ac.wellcome.platform.archive.notifier.fixtures

import java.net.URL

import akka.actor.ActorSystem
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.notifier.services.{CallbackUrlService, NotifierWorker}

import scala.concurrent.ExecutionContext.Implicits.global

trait NotifierFixtures
    extends Akka
    with AlpakkaSQSWorkerFixtures {

  def withCallbackUrlService[R](
    testWith: TestWith[CallbackUrlService, R]
  )(implicit actorSystem: ActorSystem): R = {
    val callbackUrlService = new CallbackUrlService(
      contextUrl = new URL("http://localhost/context.json")
    )
    testWith(callbackUrlService)
  }

  private def withApp[R](queue: Queue, messageSender: MemoryMessageSender)(
    testWith: TestWith[NotifierWorker[String], R]
  ): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withCallbackUrlService { callbackUrlService =>
          val workerService = new NotifierWorker(
            alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
            callbackUrlService = callbackUrlService,
            messageSender = messageSender,
            metricsNamespace = "notifier"
          )

          workerService.run()

          testWith(workerService)
        }
      }
    }

  def withNotifier[R](testWith: TestWith[(Queue, MemoryMessageSender), R]): R =
    withLocalSqsQueue { queue =>
      val messageSender = new MemoryMessageSender()
      withApp(queue = queue, messageSender = messageSender) { _ =>
        testWith((queue, messageSender))
      }
    }
}
