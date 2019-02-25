package uk.ac.wellcome.platform.archive.notifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.platform.archive.common.fixtures.BagIt
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.notifier.services.NotifierWorkerService

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends Akka
    with BagIt
    with CallbackUrlServiceFixture
    with NotificationStreamFixture
    with SNS {

  private def withApp[R](queue: Queue, topic: Topic)(
    testWith: TestWith[NotifierWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      withNotificationStream[CallbackNotification, R](queue) {
        notificationStream =>
          withCallbackUrlService { callbackUrlService =>
            withSNSWriter(topic) { snsWriter =>
              val workerService = new NotifierWorkerService(
                notificationStream = notificationStream,
                callbackUrlService = callbackUrlService,
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
