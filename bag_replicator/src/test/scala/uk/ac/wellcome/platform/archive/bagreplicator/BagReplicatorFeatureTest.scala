package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.models.PrimaryReplica
import uk.ac.wellcome.platform.archive.common.ReplicaCompletePayload
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.PrimaryS3ReplicaLocation
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix

class BagReplicatorFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with S3BagBuilder {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        val (srcBagRoot, _) = storeBagWith()(namespace = srcBucket, primaryBucket = srcBucket)

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalSqsQueue() { queue =>
          withBagReplicatorWorker(
            queue,
            bucket = dstBucket,
            ingests = ingests,
            outgoing = outgoing,
            stepName = "replicating",
            replicaType = PrimaryReplica
          ) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              val expectedDst = S3ObjectLocationPrefix(
                bucket = dstBucket.name,
                keyPrefix = Paths
                  .get(
                    payload.storageSpace.toString,
                    payload.externalIdentifier.toString,
                    payload.version.toString
                  )
                  .toString
              )

              val receivedPayload =
                outgoing
                  .getMessages[ReplicaCompletePayload]
                  .head

              receivedPayload.context shouldBe payload.context
              receivedPayload.version shouldBe payload.version

              receivedPayload.dstLocation shouldBe PrimaryS3ReplicaLocation(
                prefix = expectedDst
              )

              verifyObjectsCopied(
                srcPrefix = srcBagRoot,
                dstPrefix = expectedDst.asInstanceOf[S3ObjectLocationPrefix]
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
