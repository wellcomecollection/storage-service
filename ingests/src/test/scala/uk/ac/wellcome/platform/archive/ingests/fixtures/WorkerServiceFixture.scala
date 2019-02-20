package uk.ac.wellcome.platform.archive.ingests.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.common.progress.fixtures.ProgressTrackerFixture
import uk.ac.wellcome.platform.archive.ingests.services.IngestsWorkerService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends Akka
    with CallbackNotificationServiceFixture
    with ProgressTrackerFixture
    with SQS {
  def withWorkerService[R](queue: Queue, table: Table, topic: Topic)(
    testWith: TestWith[IngestsWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        withProgressTracker(table) { progressTracker =>
          withCallbackNotificationService(topic) {
            callbackNotificationService =>
              val service = new IngestsWorkerService(
                sqsStream = sqsStream,
                progressTracker = progressTracker,
                callbackNotificationService = callbackNotificationService
              )

              service.run()

              testWith(service)
          }
        }
      }
    }
}
