package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bagverifier.BagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.config.BagVerifierConfig
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagVerifierFixtures
    extends S3
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures {

  def withBagNotification[R](
    queue: Queue,
    storageBucket: Bucket,
    archiveRequestId: UUID
  )(testWith: TestWith[BagRequest, R]): R =
    withBag(storageBucket) { bagLocation =>
      val replicationRequest = BagRequest(
        randomUUID,
        bagLocation
      )

      sendNotificationToSQS(queue, replicationRequest)
      testWith(replicationRequest)
    }

  def withBagVerifier[R](
    queue: Queue,
    progressTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[BagVerifier, R]): R =
    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        val bagVerifier = new BagVerifier(
          s3Client = s3Client,
          snsClient = snsClient,
          sqsStream,
          bagVerifierConfig = BagVerifierConfig(
            parallelism = 10
          ),
          ingestsSnsConfig = createSNSConfigWith(progressTopic),
          outgoingSnsConfig = createSNSConfigWith(outgoingTopic)
        )

        bagVerifier.run()

        testWith(bagVerifier)
      }
    }

  def withApp[R](testWith: TestWith[(Bucket, Queue, Topic, Topic), R]): R =
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { progressTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withLocalS3Bucket { sourceBucket =>
            withBagVerifier(
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
