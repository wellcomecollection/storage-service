package uk.ac.wellcome.platform.archive.bagreplicator

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.models.{ReplicationRequest, ReplicationResult}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with BagReplicatorFixtures
    with ProgressUpdateAssertions {

  it("sends a ProgressUpdate if it replicates a bag successfully") {
    withApp {
      case (
          sourceBucket,
          queue,
          destinationBucket,
          dstRootPath,
          progressTopic,
          outgoingTopic) =>
        val requestId = randomUUID
        withBagNotification(queue, sourceBucket, requestId) { srcBagLocation =>
          eventually {
            val dstBagLocation = srcBagLocation.copy(
              storageNamespace = destinationBucket.name,
              storagePrefix = Some(dstRootPath)
            )

            verifyBagCopied(srcBagLocation, dstBagLocation)
            assertSnsReceivesOnly(
              ReplicationResult(
                archiveRequestId = requestId,
                srcBagLocation = srcBagLocation,
                dstBagLocation = dstBagLocation
              ),
              outgoingTopic
            )

            assertTopicReceivesProgressEventUpdate(requestId, progressTopic) {
              events =>
                events should have size 1
                events.head.description shouldBe s"Bag replicated successfully"
            }
          }
        }
    }
  }

  it("sends a ProgressUpdate if it cannot replicate a bag") {
    withApp {
      case (_, queue, _, _, progressTopic, outgoingTopic) =>
        val requestId = randomUUID

        val replicationRequest = ReplicationRequest(
          archiveRequestId = requestId,
          srcBagLocation = BagLocation(
            storageNamespace = randomAlphanumeric(),
            storagePrefix = randomAlphanumeric(),
            storageSpace = createStorageSpace,
            bagPath = BagPath(randomAlphanumeric())
          )
        )

        sendNotificationToSQS(queue, replicationRequest)

        eventually {
          assertSnsReceivesNothing(outgoingTopic)

          assertTopicReceivesProgressStatusUpdate(
            requestId,
            progressTopic = progressTopic,
            status = Progress.Failed) {
              events =>
                events should have size 1
                events.head.description shouldBe s"Failed to replicate bag"
          }
        }
    }
  }
}
