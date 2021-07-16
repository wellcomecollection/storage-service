package weco.storage_service.notifier.fixtures

import akka.actor.ActorSystem
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.notifier.services.{
  CallbackUrlService,
  NotifierWorker
}
import weco.http.client.{AkkaHttpClient, HttpClient}

import scala.concurrent.ExecutionContext.Implicits.global

trait NotifierFixtures extends Akka with AlpakkaSQSWorkerFixtures {

  def withCallbackUrlService[R](
    testWith: TestWith[CallbackUrlService, R]
  )(implicit actorSystem: ActorSystem): R = {
    val callbackUrlService = new CallbackUrlService(
      client = new AkkaHttpClient()
    )
    testWith(callbackUrlService)
  }

  private def withApp[R](queue: Queue, messageSender: MemoryMessageSender)(
    testWith: TestWith[NotifierWorker[String], R]
  ): R =
    withActorSystem { implicit actorSystem =>
      withCallbackUrlService { callbackUrlService =>
        implicit val metrics: MemoryMetrics = new MemoryMetrics()

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

  def withNotifier[R](testWith: TestWith[(Queue, MemoryMessageSender), R]): R =
    withLocalSqsQueue() { queue =>
      val messageSender = new MemoryMessageSender()
      withApp(queue = queue, messageSender = messageSender) { _ =>
        testWith((queue, messageSender))
      }
    }

  def withNotifier2[R](client: HttpClient)(testWith: TestWith[(Queue, MemoryMessageSender), R]): R =
    withLocalSqsQueue() { queue =>
      val messageSender = new MemoryMessageSender()

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val callbackUrlService = new CallbackUrlService(client = client)

      withActorSystem { implicit actorSystem =>
        val workerService = new NotifierWorker(
          alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          callbackUrlService = callbackUrlService,
          messageSender = messageSender,
          metricsNamespace = "notifier"
        )

        workerService.run()

        testWith((queue, messageSender))
      }
    }
}
