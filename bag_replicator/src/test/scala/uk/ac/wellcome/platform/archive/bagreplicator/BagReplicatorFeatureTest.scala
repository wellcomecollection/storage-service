package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val rootPath = randomAlphanumeric()

          val ingests = createMessageSender
          val outgoing = createMessageSender

          withLocalSqsQueue { queue =>
            withBagReplicatorWorker(queue, bucket = archiveBucket, rootPath = Some(rootPath), ingests, outgoing, stepName = "replicating") {
              _ =>
                withBag(ingestsBucket) {
                  case (srcBagRootLocation, _) =>
                    val payload = createBagInformationPayloadWith(
                      bagRootLocation = srcBagRootLocation
                    )

                    sendNotificationToSQS(queue, payload)

                    eventually {
                      val expectedDst = createObjectLocationWith(
                        bucket = archiveBucket,
                        key = Paths
                          .get(
                            rootPath,
                            payload.storageSpace.toString,
                            payload.externalIdentifier.toString,
                            s"v${payload.version}"
                          )
                          .toString
                      )

                      val expectedPayload = payload.copy(
                        bagRootLocation = expectedDst
                      )

                      println(outgoing.messages)
                      outgoing.getMessages[BagInformationPayload] shouldBe Seq(expectedPayload)

                      verifyBagCopied(
                        src = srcBagRootLocation,
                        dst = expectedDst
                      )

                      assertTopicReceivesIngestEvents(
                        payload.ingestId,
                        ingests,
                        expectedDescriptions = Seq(
                          "Replicating started",
                          "Replicating succeeded"
                        )
                      )
                    }
                }
            }
          }
      }
    }
  }
}
