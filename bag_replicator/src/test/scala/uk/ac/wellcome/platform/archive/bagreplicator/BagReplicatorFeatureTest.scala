package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.ObjectLocationPrefix

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        val (srcBagRoot, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagRoot
        )

        withLocalSqsQueue { queue =>
          withBagReplicatorWorker(
            queue,
            bucket = dstBucket,
            ingests,
            outgoing,
            stepName = "replicating"
          ) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              val expectedDst = ObjectLocationPrefix(
                namespace = dstBucket.name,
                path = Paths
                  .get(
                    payload.storageSpace.toString,
                    payload.externalIdentifier.toString,
                    payload.version.toString
                  )
                  .toString
              )

              val expectedPayload = payload.copy(
                bagRootLocation = expectedDst.asLocation()
              )

              outgoing
                .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                expectedPayload
              )

              verifyObjectsCopied(
                srcPrefix = srcBagRoot.asPrefix,
                dstPrefix = expectedDst
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
