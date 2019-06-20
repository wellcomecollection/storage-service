package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createEnrichedBagInformationPayload(
            ingestId = payload.ingestId,
            ingestDate = payload.ingestDate,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          withLocalSqsQueue { queue =>
            val ingests = new MemoryMessageSender()
            val outgoing = new MemoryMessageSender()
            withAuditorWorker(
              queue,
              ingests,
              outgoing,
              stepName = "auditing bag") { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing
                  .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                  expectedPayload)

                assertTopicReceivesIngestEvents(
                  payload.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Auditing bag started",
                    s"Detected bag root as $bagRootLocation",
                    s"Detected bag identifier as ${bagInfo.externalIdentifier}",
                    s"Assigned bag version 1",
                    "Auditing bag succeeded"
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
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo, bagRootDirectory = Some("subdir")) {
        case (unpackedBagLocation, storageSpace) =>
          val bagRootLocation = unpackedBagLocation.join("subdir")

          val payload = createUnpackedBagLocationPayloadWith(
            unpackedBagLocation = unpackedBagLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createEnrichedBagInformationPayload(
            ingestId = payload.ingestId,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          withLocalSqsQueue { queue =>
            val ingests = new MemoryMessageSender()
            val outgoing = new MemoryMessageSender()
            withAuditorWorker(
              queue,
              ingests,
              outgoing,
              stepName = "auditing bag") { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing
                  .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                  expectedPayload)

                assertTopicReceivesIngestEvents(
                  payload.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Auditing bag started",
                    s"Detected bag root as $bagRootLocation",
                    s"Detected bag identifier as ${bagInfo.externalIdentifier}",
                    s"Assigned bag version 1",
                    "Auditing bag succeeded"
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
            withAuditorWorker(
              queue,
              ingests,
              outgoing,
              stepName = "auditing bag") { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing.messages shouldBe empty

                assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
                  ingestUpdates =>
                    ingestUpdates.size shouldBe 2

                    val ingestStart = ingestUpdates.head
                    ingestStart.events.head.description shouldBe "Auditing bag started"

                    val ingestFailed =
                      ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                    ingestFailed.status shouldBe Ingest.Failed
                    ingestFailed.events.head.description shouldBe "Auditing bag failed"
                }
              }
            }
          }
      }
    }
  }

  it("errors if it cannot find the bag") {
    withLocalS3Bucket { bucket =>
      val unpackedBagLocation = createObjectLocation
      val payload = createUnpackedBagLocationPayloadWith(unpackedBagLocation)

      withLocalSqsQueue { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withAuditorWorker(queue, ingests, outgoing, stepName = "auditing bag") {
          _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              assertQueueEmpty(queue)

              outgoing.messages shouldBe empty

              assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
                ingestUpdates =>
                  ingestUpdates.size shouldBe 2

                  val ingestStart = ingestUpdates.head
                  ingestStart.events.head.description shouldBe "Auditing bag started"

                  val ingestFailed =
                    ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                  ingestFailed.status shouldBe Ingest.Failed
                  ingestFailed.events.head.description shouldBe "Auditing bag failed"
              }
            }
        }
      }
    }
  }

  it("errors if it gets an error from S3") {
    val unpackedBagLocation = createObjectLocation
    val payload = createUnpackedBagLocationPayloadWith(unpackedBagLocation)

    withLocalSqsQueue { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()
      withAuditorWorker(queue, ingests, outgoing, stepName = "auditing bag") {
        _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing.messages shouldBe empty

            assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates.size shouldBe 2

                val ingestStart = ingestUpdates.head
                ingestStart.events.head.description shouldBe "Auditing bag started"

                val ingestFailed =
                  ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                ingestFailed.status shouldBe Ingest.Failed
                ingestFailed.events.head.description shouldBe "Auditing bag failed"
            }
          }
      }
    }
  }
}
