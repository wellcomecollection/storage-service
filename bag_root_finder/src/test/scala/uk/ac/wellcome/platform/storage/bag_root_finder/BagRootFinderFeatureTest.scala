package uk.ac.wellcome.platform.storage.bag_root_finder

import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.BagRootLocationPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bag_root_finder.fixtures.BagRootFinderFixtures

class BagRootFinderFeatureTest
    extends AnyFunSpec
    with BagRootFinderFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with S3BagBuilder {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      val (unpackedBagRoot, _) = storeBagWith()(namespace = bucket, primaryBucket = bucket)

      val payload = createUnpackedBagLocationPayloadWith(
        unpackedBagLocation = unpackedBagRoot
      )

      val expectedPayload = createBagRootLocationPayloadWith(
        context = payload.context,
        bagRoot = unpackedBagRoot
      )

      withLocalSqsQueue() { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withWorkerService(
          queue,
          ingests,
          outgoing,
          stepName = "finding bag root"
        ) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing.getMessages[BagRootLocationPayload] shouldBe Seq(
              expectedPayload
            )

            assertTopicReceivesIngestEvents(
              payload.ingestId,
              ingests,
              expectedDescriptions = Seq(
                "Finding bag root started",
                "Finding bag root succeeded"
              )
            )
          }
        }
      }
    }
  }

  it("detects a bag in a subdirectory of the bagLocation") {
    withLocalS3Bucket { bucket =>
      val builder = new S3BagBuilder {
        override protected def createBagRootPath(
          space: StorageSpace,
          externalIdentifier: ExternalIdentifier,
          version: BagVersion
        ): String = {
          val rootPath =
            super.createBagRootPath(space, externalIdentifier, version)

          Seq(rootPath, "subdir").mkString("/")
        }
      }

      val (unpackedBagRoot, _) = builder.storeBagWith()(namespace = bucket, primaryBucket = bucket)

      val (parentDirectory, _) = unpackedBagRoot.keyPrefix.splitAt(
        unpackedBagRoot.keyPrefix.lastIndexOf("/")
      )

      val parentLocation = unpackedBagRoot.copy(
        keyPrefix = parentDirectory
      )

      val payload = createUnpackedBagLocationPayloadWith(
        unpackedBagLocation = parentLocation
      )

      val expectedPayload = createBagRootLocationPayloadWith(
        context = payload.context,
        bagRoot = unpackedBagRoot
      )

      withLocalSqsQueue() { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withWorkerService(
          queue,
          ingests,
          outgoing,
          stepName = "finding bag root"
        ) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing
              .getMessages[BagRootLocationPayload] shouldBe Seq(expectedPayload)

            assertTopicReceivesIngestEvents(
              payload.ingestId,
              ingests,
              expectedDescriptions = Seq(
                "Finding bag root started",
                "Finding bag root succeeded"
              )
            )
          }
        }
      }
    }
  }

  it("errors if the bag is nested too deep") {
    withLocalS3Bucket { bucket =>
      val (unpackedBagRoot, _) = storeBagWith()(namespace = bucket, primaryBucket = bucket)

      val bucketRootLocation = unpackedBagRoot.copy(keyPrefix = "")

      val payload =
        createUnpackedBagLocationPayloadWith(bucketRootLocation)

      withLocalSqsQueue() { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withWorkerService(
          queue,
          ingests,
          outgoing,
          stepName = "finding bag root"
        ) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing.messages shouldBe empty

            assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates.size shouldBe 2

                val ingestStart = ingestUpdates.head
                ingestStart.events.head.description shouldBe "Finding bag root started"

                val ingestFailed =
                  ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                ingestFailed.status shouldBe Ingest.Failed
                ingestFailed.events.head.description shouldBe s"Finding bag root failed"
            }
          }
        }
      }
    }
  }

  it("errors if it cannot find the bag") {
    val payload =
      createUnpackedBagLocationPayloadWith(createS3ObjectLocationPrefix)

    withLocalSqsQueue() { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()
      withWorkerService(queue, ingests, outgoing, stepName = "finding bag root") {
        _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing.messages shouldBe empty

            assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates.size shouldBe 2

                val ingestStart = ingestUpdates.head
                ingestStart.events.head.description shouldBe "Finding bag root started"

                val ingestFailed =
                  ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                ingestFailed.status shouldBe Ingest.Failed
                ingestFailed.events.head.description shouldBe s"Finding bag root failed"
            }
          }
      }
    }
  }
}
