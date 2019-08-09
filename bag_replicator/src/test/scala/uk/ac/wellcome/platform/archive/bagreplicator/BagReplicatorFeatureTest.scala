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
        val rootPath = randomAlphanumericWithLength()

        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagLocation
        )

        withLocalSqsQueue { queue =>
          withBagReplicatorWorker(
            queue,
            bucket = dstBucket,
            rootPath = Some(rootPath),
            ingests,
            outgoing,
            stepName = "replicating"
          ) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              val expectedDst = createObjectLocationWith(
                bucket = dstBucket,
                key = Paths
                  .get(
                    rootPath,
                    payload.storageSpace.toString,
                    payload.externalIdentifier.toString,
                    payload.version.toString
                  )
                  .toString
              )

              val expectedPayload = payload.copy(
                bagRootLocation = expectedDst
              )

              outgoing
                .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                expectedPayload
              )

              verifyObjectsCopied(
                src = srcBagLocation,
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
