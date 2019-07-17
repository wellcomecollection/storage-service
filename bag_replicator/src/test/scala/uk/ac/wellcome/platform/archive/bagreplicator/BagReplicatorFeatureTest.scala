package uk.ac.wellcome.platform.archive.bagreplicator

import java.nio.file.Paths

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.{
  BagReplicaLocation,
  BagReplicaLocationPayload
}
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class BagReplicatorFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with S3BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val rootPath = randomAlphanumericWithLength()

        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        withLocalSqsQueue { queue =>
          withBagReplicatorWorker(
            queue,
            bucket = archiveBucket,
            rootPath = Some(rootPath),
            ingests,
            outgoing,
            stepName = "replicating") { _ =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createVersionedBagRootLocationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              sendNotificationToSQS(queue, payload)

              eventually {
                val expectedDst = createObjectLocationWith(
                  bucket = archiveBucket,
                  key = Paths
                    .get(
                      rootPath,
                      payload.storageSpace.toString,
                      payload.externalIdentifier.toString
                    )
                    .toString
                )

                val expectedPayload = BagReplicaLocationPayload(
                  context = payload.context,
                  version = payload.version,
                  replicaLocation = BagReplicaLocation(
                    bagRoot = expectedDst,
                    versionDirectory = s"v${payload.version}"
                  )
                )

                outgoing
                  .getMessages[BagReplicaLocationPayload] shouldBe Seq(
                  expectedPayload)

                verifyObjectsCopied(
                  src = bagRootLocation,
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
