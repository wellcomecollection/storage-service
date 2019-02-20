package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bagverifier.BagReplicator
import uk.ac.wellcome.platform.archive.bagverifier.config.{
  BagReplicatorConfig,
  ReplicatorDestinationConfig
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  ArchiveMessaging,
  BagLocationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.models._
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.collection.JavaConverters._

trait BagReplicatorFixtures
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
      val replicationRequest = ReplicationRequest(
        archiveRequestId = archiveRequestId,
        srcBagLocation = bagLocation
      )

      sendNotificationToSQS(queue, replicationRequest)
      testWith(bagLocation)
    }

  def withBagReplicator[R](
    queue: Queue,
    progressTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket,
    dstRootPath: String)(testWith: TestWith[BagReplicator, R]): R =
    withActorSystem { implicit actorSystem =>
      withArchiveMessageStream[NotificationMessage, Unit, R](queue) {
        messageStream =>
          val bagReplicator = new BagReplicator(
            s3Client = s3Client,
            snsClient = snsClient,
            messageStream = messageStream,
            bagReplicatorConfig = BagReplicatorConfig(
              parallelism = 10,
              ReplicatorDestinationConfig(dstBucket.name, dstRootPath)),
            progressSnsConfig = createSNSConfigWith(progressTopic),
            outgoingSnsConfig = createSNSConfigWith(outgoingTopic)
          )

          bagReplicator.run()

          testWith(bagReplicator)
      }
    }

  def withApp[R](
    testWith: TestWith[(Bucket, Queue, Bucket, String, Topic, Topic), R]): R =
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { progressTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withLocalS3Bucket { sourceBucket =>
            withLocalS3Bucket { destinationBucket =>
              val dstRootPath = "storage-root"
              withBagReplicator(
                queue,
                progressTopic,
                outgoingTopic,
                destinationBucket,
                dstRootPath)({ _ =>
                testWith(
                  (
                    sourceBucket,
                    queue,
                    destinationBucket,
                    dstRootPath,
                    progressTopic,
                    outgoingTopic))
              })
            }
          }
        }
      }
    }

  def verifyBagCopied(src: BagLocation, dst: BagLocation): Assertion = {
    val sourceItems = getObjectSummaries(src)
    val sourceKeyEtags = sourceItems.map { _.getETag }

    val destinationItems = getObjectSummaries(dst)
    val destinationKeyEtags = destinationItems.map { _.getETag }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }

  private def getObjectSummaries(
    bagLocation: BagLocation): List[S3ObjectSummary] =
    s3Client
      .listObjects(bagLocation.storageNamespace, bagLocation.completePath)
      .getObjectSummaries
      .asScala
      .toList

}
