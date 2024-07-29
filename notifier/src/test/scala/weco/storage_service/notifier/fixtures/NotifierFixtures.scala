package weco.storage_service.notifier.fixtures

import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.PekkoSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.notifier.services.{
  CallbackUrlService,
  NotifierWorker
}
import weco.http.client.HttpClient

import scala.concurrent.ExecutionContext.Implicits.global

trait NotifierFixtures extends Pekko with PekkoSQSWorkerFixtures {
  def withNotifier[R](
    client: HttpClient
  )(testWith: TestWith[(Queue, MemoryMessageSender), R]): R =
    withLocalSqsQueue() { queue =>
      val messageSender = new MemoryMessageSender()

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val callbackUrlService = new CallbackUrlService(client = client)

      withActorSystem { implicit actorSystem =>
        val workerService = new NotifierWorker(
          config = createPekkoSQSWorkerConfig(queue),
          callbackUrlService = callbackUrlService,
          messageSender = messageSender
        )

        workerService.run()

        testWith((queue, messageSender))
      }
    }
}
