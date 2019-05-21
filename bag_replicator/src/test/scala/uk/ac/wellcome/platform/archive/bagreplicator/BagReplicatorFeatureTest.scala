package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.ObjectLocation

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val destination = createReplicatorDestinationConfigWith(archiveBucket)

        val ingests = createMessageSender
        val outgoing = createMessageSender

        withLocalSqsQueue { queue =>
          withBagReplicatorWorker(
            queue,
            ingests,
            outgoing,
            config = destination) { _ =>
            withBag(storageBackend, namespace = ingestsBucket.name) {
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
                    bagRootLocation = expectedDst
                  )

                  outgoing.getMessages[BagInformationPayload]() shouldBe Seq(
                    expectedPayload)

                  verifyBagCopied(
                    src = srcBagRootLocation,
                    dst = expectedDst
                  )

                  assertReceivesIngestEvents(ingests)(
                    payload.ingestId,
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
