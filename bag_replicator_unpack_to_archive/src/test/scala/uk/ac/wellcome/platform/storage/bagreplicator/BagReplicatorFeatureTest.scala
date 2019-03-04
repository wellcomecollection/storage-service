package uk.ac.wellcome.platform.storage.bagreplicator

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.storage.bagreplicator.fixtures.WorkerServiceFixture

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagReplicatorFixtures
    with BagRequestGenerators
    with ProgressUpdateAssertions
    with WorkerServiceFixture {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        withLocalSqsQueue { queue =>
          withLocalSnsTopic { progressTopic =>
            withLocalSnsTopic { outgoingTopic =>
              withWorkerService(
                queue,
                progressTopic = progressTopic,
                outgoingTopic = outgoingTopic) { _ =>
                val bagInfo = createBagInfo

                withBag(ingestsBucket, bagInfo = bagInfo) { srcBagLocation =>
                  val bagRequest = createBagRequestWith(srcBagLocation)

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

                    dstBagLocation shouldBe BagLocation(
                      storageNamespace = archiveBucket.name,
                      storagePrefix = None,
                      storageSpace = srcBagLocation.storageSpace,
                      bagPath = BagPath(bagInfo.externalIdentifier.underlying)
                    )

                    verifyBagCopied(
                      src = srcBagLocation,
                      dst = dstBagLocation
                    )

                    assertTopicReceivesProgressEventUpdate(
                      bagRequest.archiveRequestId,
                      progressTopic) { events =>
                      events should have size 1
                      events.head.description shouldBe "Bag successfully copied to archive bucket"
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
}
