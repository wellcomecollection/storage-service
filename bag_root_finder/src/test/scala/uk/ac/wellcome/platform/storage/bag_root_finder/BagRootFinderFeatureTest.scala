package uk.ac.wellcome.platform.storage.bag_root_finder

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.BagRootPayload
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagLocationFixtures
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
    with S3BagLocationFixtures {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      withS3Bag(bucket) {
        case (unpackedBagLocation, _) =>
          // TODO: Bag root location should really be a prefix here
          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = unpackedBagLocation.asPrefix
          )

          val expectedPayload = createBagRootLocationPayloadWith(
            context = payload.context,
            bagRootLocation = unpackedBagLocation
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
      withS3Bag(bucket, bagRootDirectory = Some("subdir")) {
        case (unpackedBagLocation, _) =>
          val bagRootLocation = unpackedBagLocation.join("subdir")

          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = unpackedBagLocation.asPrefix
          )

          val expectedPayload = createBagRootLocationPayloadWith(
            context = payload.context,
            bagRootLocation = bagRootLocation
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
      withS3Bag(bucket, bagRootDirectory = Some("subdir1/subdir2/subdir3")) {
        case (unpackedBagLocation, _) =>
          val payload =
            createUnpackedBagLocationPayloadWith(unpackedBagLocation.asPrefix)

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
                    ingestFailed.events.head.description shouldBe s"Finding bag root failed"
                }
              }
            }
          }
      }
    }
  }

  it("errors if it cannot find the bag") {
    val unpackedBagLocation = createObjectLocation
    val payload =
      createUnpackedBagLocationPayloadWith(unpackedBagLocation.asPrefix)

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
                ingestFailed.events.head.description shouldBe s"Finding bag root failed"
            }
          }
      }
    }
  }
}
