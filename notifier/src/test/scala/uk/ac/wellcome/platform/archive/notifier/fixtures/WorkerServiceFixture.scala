package uk.ac.wellcome.platform.archive.notifier.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagIt,
  MonitoringClientFixture
}
import uk.ac.wellcome.platform.archive.notifier.services.NotifierWorker

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends BagIt
    with CallbackUrlServiceFixture
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with SNS {

  private def withApp[R](queue: Queue, topic: Topic)(
    testWith: TestWith[NotifierWorker, R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer(actorSystem) { implicit materializer =>
          withCallbackUrlService { callbackUrlService =>
            withSNSWriter(topic) { snsWriter =>
              val workerService = new NotifierWorker(
                alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
                callbackUrlService = callbackUrlService,
                snsWriter = snsWriter
              )

              workerService.run()

              testWith(workerService)
            }
          }
        }
      }
    }

  def withNotifier[R](testWith: TestWith[(Queue, Topic), R]): R =
    withLocalSqsQueueAndDlqAndTimeout(visibilityTimeout = 15) { queuePair =>
      withLocalSnsTopic { topic =>
        withApp(queue = queuePair.queue, topic = topic) { _ =>
          testWith((queuePair.queue, topic))
        }
      }
    }
}
