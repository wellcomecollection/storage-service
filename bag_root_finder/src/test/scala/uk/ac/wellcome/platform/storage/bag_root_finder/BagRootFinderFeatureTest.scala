package uk.ac.wellcome.platform.storage.bag_root_finder

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.BagRootPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.storage.bag_root_finder.fixtures.BagRootFinderFixtures

class BagRootFinderFeatureTest
    extends FunSpec
    with BagRootFinderFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with BagLocationFixtures {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createBagRootLocationPayloadWith(
            ingestId = payload.ingestId,
            ingestDate = payload.ingestDate,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          withLocalSqsQueue { queue =>
            val ingests = new MemoryMessageSender()
            val outgoing = new MemoryMessageSender()
            withWorkerService(
              queue,
              ingests,
              outgoing,
              stepName = "finding bag root") { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing.getMessages[BagRootPayload] shouldBe Seq(
                  expectedPayload)

                assertTopicReceivesIngestEvents(
                  payload.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Finding bag root started",
                    s"Detected bag root as $bagRootLocation",
                    "Finding bag root succeeded"
                  )
                )
              }
            }
          }
      }
    }
  }

  it("detects a bag in a subdirectory of the bagLocation") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, bagRootDirectory = Some("subdir")) {
        case (unpackedBagLocation, storageSpace) =>
          val bagRootLocation = unpackedBagLocation.join("subdir")

          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = unpackedBagLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createBagRootLocationPayloadWith(
            ingestId = payload.ingestId,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          withLocalSqsQueue { queue =>
            val ingests = new MemoryMessageSender()
            val outgoing = new MemoryMessageSender()
            withWorkerService(
              queue,
              ingests,
              outgoing,
              stepName = "finding bag root") { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing
                  .getMessages[BagRootPayload] shouldBe Seq(expectedPayload)

                assertTopicReceivesIngestEvents(
                  payload.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Finding bag root started",
                    s"Detected bag root as $bagRootLocation",
                    "Finding bag root succeeded"
                  )
                )
              }
            }
          }
      }
    }
  }

  it("errors if the bag is nested too deep") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, bagRootDirectory = Some("subdir1/subdir2/subdir3")) {
        case (unpackedBagLocation, _) =>
          val payload =
            createUnpackedBagLocationPayloadWith(unpackedBagLocation)

          withLocalSqsQueue { queue =>
            val ingests = new MemoryMessageSender()
            val outgoing = new MemoryMessageSender()
            withWorkerService(
              queue,
              ingests,
              outgoing,
              stepName = "finding bag root") { _ =>
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
                    ingestFailed.events.head.description shouldBe s"Finding bag root failed - Unable to find root of the bag at $unpackedBagLocation"
                }
              }
            }
          }
      }
    }
  }

  it("errors if it cannot find the bag") {
    val unpackedBagLocation = createObjectLocation
    val payload = createUnpackedBagLocationPayloadWith(unpackedBagLocation)

    withLocalSqsQueue { queue =>
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
                ingestFailed.events.head.description shouldBe s"Finding bag root failed - Unable to find root of the bag at $unpackedBagLocation"
            }
          }
      }
    }
  }
}
