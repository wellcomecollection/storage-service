package uk.ac.wellcome.platform.archive.bagreplicator

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, ReplicationResult}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagReplicatorFixtures
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
              outgoingTopic = outgoingTopic) { service =>
              withBag(bucket) { srcBagLocation =>
                val bagRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = srcBagLocation
                )

                sendNotificationToSQS(queue, bagRequest)

                eventually {
                  val outgoingMessages =
                    listMessagesReceivedFromSNS(outgoingTopic)
                  val results =
                    outgoingMessages.map { msg =>
                      fromJson[ReplicationResult](msg.message).get
                    }.distinct

                  results should have size 1
                  val result = results.head
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
