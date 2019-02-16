package uk.ac.wellcome.platform.archive.notifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{SNS, SQS}
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.common.fixtures.BagIt
import uk.ac.wellcome.platform.archive.notifier.services.NotifierWorkerService

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends Akka
    with BagIt
    with CallbackUrlServiceFixture
    with SNS
    with SQS {

  private def withApp[R](queue: Queue, topic: Topic)(
    testWith: TestWith[NotifierWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      withCallbackUrlService { callbackUrlService =>
        withSQSStream[NotificationMessage, R](queue) { sqsStream =>
          withSNSWriter(topic) { snsWriter =>
            val workerService = new NotifierWorkerService(
              callbackUrlService = callbackUrlService,
              sqsStream = sqsStream,
              snsWriter = snsWriter
            )

            workerService.run()

            testWith(workerService)
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
