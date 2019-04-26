package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.ObjectLocation

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

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
                  val payload = createObjectLocationPayloadWith(
                    objectLocation = srcBagLocation.objectLocation,
                    storageSpace = srcBagLocation.storageSpace
                  )

                  sendNotificationToSQS(queue, payload)

                  eventually {
                    val expectedPayload = payload.copy(
                      objectLocation = ObjectLocation(
                        namespace = destination.namespace,
                        key = Paths
                          .get(
                            destination.rootPath.getOrElse(""),
                            srcBagLocation.storageSpace.toString,
                            bagInfo.externalIdentifier.toString
                          )
                          .toString
                      )
                    )

                    assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                    verifyBagCopied(
                      src = srcBagLocation.objectLocation,
                      dst = expectedPayload.objectLocation
                    )

                    assertTopicReceivesIngestEvent(
                      payload.ingestId,
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
