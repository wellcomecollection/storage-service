package uk.ac.wellcome.platform.archive.bagreplicator.archive_to_access

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.archive_to_access.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with BagRequestGenerators
    with ProgressUpdateAssertions
    with WorkerServiceFixture {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { bucket =>
      withLocalSqsQueue { queue =>
        withLocalSnsTopic { progressTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withWorkerService(
              queue,
              progressTopic = progressTopic,
              outgoingTopic = outgoingTopic) { _ =>
              withBag(bucket) { srcBagLocation =>
                val bagRequest = createBagRequestWith(srcBagLocation)

                sendNotificationToSQS(queue, bagRequest)

                eventually {
                  val result =
                    notificationMessage[ReplicationResult](outgoingTopic)
                  result.archiveRequestId shouldBe bagRequest.archiveRequestId
                  result.srcBagLocation shouldBe bagRequest.bagLocation

                  val dstBagLocation = result.dstBagLocation

                  verifyBagCopied(
                    src = srcBagLocation,
                    dst = dstBagLocation
                  )

                  assertTopicReceivesProgressEventUpdate(
                    bagRequest.archiveRequestId,
                    progressTopic) { events =>
                    events should have size 1
                    events.head.description shouldBe "Bag replicated successfully"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
