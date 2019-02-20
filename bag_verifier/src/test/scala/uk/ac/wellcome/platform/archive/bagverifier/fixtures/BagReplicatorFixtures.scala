package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.bagverifier.BagVerifier
import uk.ac.wellcome.platform.archive.bagreplicator.config.BagVerifierConfig
import uk.ac.wellcome.platform.archive.common.fixtures.{ArchiveMessaging, BagLocationFixtures, RandomThings}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagVerifierFixtures
  extends S3
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures
    with ArchiveMessaging {

  def withBagNotification[R](
                              queue: Queue,
                              storageBucket: Bucket,
                              archiveRequestId: UUID = randomUUID
                            )(testWith: TestWith[BagLocation, R]): R =
    withBag(storageBucket) { bagLocation =>

      sendNotificationToSQS(queue, bagLocation)
      testWith(bagLocation)
    }

  def withBagVerifier[R](
                          queue: Queue,
                          progressTopic: Topic,
                          outgoingTopic: Topic,
                          dstBucket: Bucket
                        )(testWith: TestWith[BagVerifier, R]): R =
    withActorSystem { implicit actorSystem =>
      val bagVerifier = new BagVerifier(
        s3Client = s3Client,
        snsClient = snsClient,
        bagVerifierConfig = BagVerifierConfig(
          parallelism = 10
        ),
        progressSnsConfig =
          createSNSConfigWith(progressTopic),
        outgoingSnsConfig =
          createSNSConfigWith(outgoingTopic)
      )

      bagVerifier.run()

      testWith(bagVerifier)
    }


  def withApp[R](
                  testWith: TestWith[(
                    Bucket,
                      Queue,
                      Topic,
                      Topic), R]): R =

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
