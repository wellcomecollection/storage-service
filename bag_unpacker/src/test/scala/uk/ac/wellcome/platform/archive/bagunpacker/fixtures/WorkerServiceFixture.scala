package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{Messaging, NotificationStreamFixture}
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorkerService
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.models.{
  StorageSpace,
  UnpackBagRequest
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait WorkerServiceFixture
    extends S3
    with NotificationStreamFixture
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures {

  def withBagNotification[R](
    queue: Queue,
    storageBucket: Bucket,
    archiveRequestId: UUID
  )(testWith: TestWith[UnpackBagRequest, R]): R = {
    val unpackBagRequest = UnpackBagRequest(
      requestId = randomUUID,
      sourceLocation = ObjectLocation(
        namespace = storageBucket.name,
        key = "not_a_real_file"
      ),
      storageSpace = StorageSpace(randomAlphanumeric())
    )

    sendNotificationToSQS(queue, unpackBagRequest)
    testWith(unpackBagRequest)
  }

  def withBagUnpacker[R](
    queue: Queue,
    progressTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[BagUnpackerWorkerService, R]): R =
    withSNSWriter(progressTopic) { progressSnsWriter =>
      withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
        withNotificationStream[UnpackBagRequest, R](queue) {
          notificationStream =>
            val bagUnpacker = new BagUnpackerWorkerService(
              stream = notificationStream,
              progressSnsWriter = progressSnsWriter,
              outgoingSnsWriter = outgoingSnsWriter
            )

            bagUnpacker.run()

            testWith(bagUnpacker)
        }
      }

    }

  def withApp[R](testWith: TestWith[(Bucket, Queue, Topic, Topic), R]): R =
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { progressTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withLocalS3Bucket { sourceBucket =>
            withBagUnpacker(
              queue,
              progressTopic,
              outgoingTopic,
              sourceBucket
            )({ _ =>
              testWith(
                (
                  sourceBucket,
                  queue,
                  progressTopic,
                  outgoingTopic
                )
              )
            })
          }
        }
      }
    }
}
