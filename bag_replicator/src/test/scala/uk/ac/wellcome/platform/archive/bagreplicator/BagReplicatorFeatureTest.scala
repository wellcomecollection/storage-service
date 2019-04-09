package uk.ac.wellcome.platform.archive.bagreplicator

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with BagRequestGenerators
    with IngestUpdateAssertions {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val destination = createReplicatorDestinationConfigWith(archiveBucket)

        withLocalSqsQueue { queue =>
          withLocalSnsTopic { ingestTopic =>
            withLocalSnsTopic { outgoingTopic =>
              withBagReplicatorWorker(
                queue,
                ingestTopic = ingestTopic,
                outgoingTopic = outgoingTopic,
                config = destination) { _ =>
                val bagInfo = createBagInfo

                withBag(ingestsBucket, bagInfo = bagInfo) { srcBagLocation =>
                  val bagRequest = createBagRequestWith(srcBagLocation)

                  sendNotificationToSQS(queue, bagRequest)

                  eventually {
                    val result = notificationMessage[BagRequest](outgoingTopic)
                    result.requestId shouldBe bagRequest.requestId

                    val dstBagLocation = result.bagLocation

                    dstBagLocation shouldBe BagLocation(
                      storageNamespace = destination.namespace,
                      storagePrefix = destination.rootPath,
                      storageSpace = srcBagLocation.storageSpace,
                      bagPath = BagPath(bagInfo.externalIdentifier.underlying)
                    )

                    verifyBagCopied(
                      src = srcBagLocation,
                      dst = dstBagLocation
                    )

                    assertTopicReceivesIngestEvent(
                      bagRequest.requestId,
                      ingestTopic) { events =>
                      events should have size 1
                      events.head.description shouldBe "Replicating succeeded"
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
