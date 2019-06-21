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

  it("audits a bag") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          val payload = createBagRootLocationPayloadWith(
            bagRootLocation = bagRootLocation,
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

  it("errors if it cannot find the bag") {
    val payload = createBagRootLocationPayload

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
                ingestFailed.events.head.description shouldBe "Auditing bag failed - Unable to find an external identifier"
            }
          }
      }
    }
  }

  // TODO: When we pass an ingest type in the bag auditor payload, check it sends
  // an appropriate user-facing message in the ingests app.
}
