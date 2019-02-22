package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bagunpacker.BagUnpacker
import uk.ac.wellcome.platform.archive.bagunpacker.config.BagUnpackerConfig
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.models.{
  StorageSpace,
  UnpackBagRequest
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagUnpackerFixtures
    extends S3
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
      packedBagLocation = ObjectLocation(
        namespace = storageBucket.name,
        key = "not_a_real_file"
      ),
      bagDestination = BagLocation(
        storageNamespace = randomAlphanumeric(),
        storagePrefix = None,
        storageSpace = StorageSpace(randomAlphanumeric()),
        bagPath = randomBagPath
      )
    )

    sendNotificationToSQS(queue, unpackBagRequest)
    testWith(unpackBagRequest)
  }

  def withBagUnpacker[R](
    queue: Queue,
    progressTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[BagUnpacker, R]): R =
    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        val bagUnpacker = new BagUnpacker(
          s3Client = s3Client,
          snsClient = snsClient,
          sqsStream,
          bagUnpackerConfig = BagUnpackerConfig(
            parallelism = 10
          ),
          ingestsSnsConfig = createSNSConfigWith(progressTopic),
          outgoingSnsConfig = createSNSConfigWith(outgoingTopic)
        )

        bagUnpacker.run()

        testWith(bagUnpacker)
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
