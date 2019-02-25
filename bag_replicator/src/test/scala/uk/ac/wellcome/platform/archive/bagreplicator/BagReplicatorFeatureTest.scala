package uk.ac.wellcome.platform.archive.bagreplicator

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.{
  BagReplicatorFixtures,
  WorkerServiceFixture
}
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  ReplicationResult
}
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
                val replicationRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = srcBagLocation
                )

                sendNotificationToSQS(queue, replicationRequest)

                eventually {
                  val outgoingMessages =
                    listMessagesReceivedFromSNS(outgoingTopic)
                  val results =
                    outgoingMessages.map { msg =>
                      fromJson[ReplicationResult](msg.message).get
                    }.distinct

                  results should have size 1
                  val result = results.head
                  result.archiveRequestId shouldBe replicationRequest.archiveRequestId
                  result.srcBagLocation shouldBe replicationRequest.bagLocation

                  val dstBagLocation = result.dstBagLocation

                  verifyBagCopied(
                    src = srcBagLocation,
                    dst = dstBagLocation
                  )

                  assertTopicReceivesProgressEventUpdate(
                    replicationRequest.archiveRequestId,
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
