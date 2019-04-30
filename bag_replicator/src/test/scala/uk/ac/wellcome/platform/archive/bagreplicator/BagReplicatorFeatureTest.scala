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
                withBag(ingestsBucket) {
                  case (srcBagRootLocation, _) =>
                    val payload = createBagInformationPayloadWith(
                      bagRootLocation = srcBagRootLocation
                    )

                    sendNotificationToSQS(queue, payload)

                    eventually {
                      val expectedDst = ObjectLocation(
                        namespace = destination.namespace,
                        key = Paths
                          .get(
                            destination.rootPath.getOrElse(""),
                            payload.storageSpace.toString,
                            payload.externalIdentifier.toString,
                            s"v${payload.version}"
                          )
                          .toString
                      )

                      val expectedPayload = payload.copy(
                        objectLocation = expectedDst
                      )

                      assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                      verifyBagCopied(
                        src = srcBagRootLocation,
                        dst = expectedDst
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
