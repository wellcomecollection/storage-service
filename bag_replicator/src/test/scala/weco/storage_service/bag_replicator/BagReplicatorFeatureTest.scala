package weco.storage_service.bag_replicator

import java.nio.file.Paths

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.bag_replicator.fixtures.BagReplicatorFixtures
import weco.storage_service.bag_replicator.models.PrimaryReplica
import weco.storage_service.ReplicaCompletePayload
import weco.storage_service.fixtures.s3.S3BagBuilder
import weco.storage_service.generators.PayloadGenerators
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.storage.models.PrimaryS3ReplicaLocation
import weco.storage.s3.S3ObjectLocationPrefix

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

        val (srcBagRoot, _) =
          storeBagWith()(namespace = srcBucket, primaryBucket = srcBucket)

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
