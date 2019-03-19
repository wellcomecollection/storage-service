package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{Messaging, NotificationStreamFixture}
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{S3Uploader, Unpacker, UnpackerWorker}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, OperationFixtures, RandomThings}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends S3
    with NotificationStreamFixture
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures
    with OperationFixtures {

  def withBagNotification[R](
    queue: Queue,
    storageBucket: Bucket,
    requestId: UUID,
    testArchive: TestArchive
  )(testWith: TestWith[UnpackBagRequest, R]): R = {
    val unpackBagRequest = UnpackBagRequest(
      requestId = randomUUID,
      sourceLocation = testArchive.location,
      storageSpace = StorageSpace(randomAlphanumeric())
    )

    sendNotificationToSQS(queue, unpackBagRequest)
    testWith(unpackBagRequest)
  }

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingestTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[UnpackerWorker, R]): R =
    withNotificationStream[UnpackBagRequest, R](queue) { notificationStream =>
      withIngestUpdater("unpacker", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("unpacker", outgoingTopic) { outgoingPublisher =>
          withOperationReporter() { reporter =>
            val bagUnpackerConfig = UnpackerWorkerConfig(dstBucket.name)
            val bagUnpacker = new UnpackerWorker(
              bagUnpackerConfig,
              notificationStream,
              ingestUpdater,
              outgoingPublisher,
              reporter,
              Unpacker(
                s3Uploader = new S3Uploader()
              )
            )

            bagUnpacker.run()

            testWith(bagUnpacker)
          }
        }
      }
    }

  def withApp[R](
    testWith: TestWith[(UnpackerWorker, Bucket, Queue, Topic, Topic), R]): R =
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { ingestTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withLocalS3Bucket { sourceBucket =>
            withBagUnpackerWorker(
              queue,
              ingestTopic,
              outgoingTopic,
              sourceBucket
            )({ service =>
              testWith(
                (
                  service,
                  sourceBucket,
                  queue,
                  ingestTopic,
                  outgoingTopic
                )
              )
            })
          }
        }
      }
    }
}
