package weco.storage_service.notifier.fixtures

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
import weco.http.client.HttpClient

import scala.concurrent.ExecutionContext.Implicits.global

trait NotifierFixtures extends Akka with AlpakkaSQSWorkerFixtures {
  def withNotifier[R](
    client: HttpClient
  )(testWith: TestWith[(Queue, MemoryMessageSender), R]): R =
    withLocalSqsQueue() { queue =>
      val messageSender = new MemoryMessageSender()

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val callbackUrlService = new CallbackUrlService(client = client)

      withActorSystem { implicit actorSystem =>
        val workerService = new NotifierWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          callbackUrlService = callbackUrlService,
          messageSender = messageSender
        )

        workerService.run()

        testWith((queue, messageSender))
      }
    }
}
