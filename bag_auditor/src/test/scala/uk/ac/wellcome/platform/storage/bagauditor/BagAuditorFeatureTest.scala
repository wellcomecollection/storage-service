package uk.ac.wellcome.platform.storage.bagauditor

import java.time.Instant

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  EnrichedBagInformationPayload
}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("audits a bag") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          val payload = createBagRootLocationPayloadWith(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createEnrichedBagInformationPayloadWith(
            context = payload.context,
            bagRootLocation = bagRootLocation,
            version = 1
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
                    "Assigned bag version 1",
                    "Auditing bag succeeded"
                  )
                )
              }
            }
          }
      }
    }
  }

  it("assigns a version for an updated bag") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          val payload1 = BagRootLocationPayload(
            context = createPipelineContextWith(
              ingestId = createIngestID,
              ingestType = CreateIngestType,
              ingestDate = Instant.ofEpochSecond(1),
              storageSpace = storageSpace
            ),
            bagRootLocation = bagRootLocation
          )

          val payload2 = payload1.copy(
            context = payload1.context.copy(
              ingestId = createIngestID,
              ingestType = UpdateIngestType,
              ingestDate = Instant.ofEpochSecond(2)
            )
          )

          val ingests = new MemoryMessageSender()
          val outgoing = new MemoryMessageSender()

          withLocalSqsQueue { queue =>
            withAuditorWorker(
              queue,
              ingests,
              outgoing,
              stepName = "auditing bag") { _ =>
              // Send the initial payload with "create" and check it completes
              sendNotificationToSQS(queue, payload1)

              eventually {
                assertQueueEmpty(queue)

                outgoing
                  .getMessages[EnrichedBagInformationPayload] should have size 1

                assertTopicReceivesIngestEvents(
                  payload1.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Auditing bag started",
                    "Assigned bag version 1",
                    "Auditing bag succeeded"
                  )
                )
              }

              // Now send the payload with "update"
              sendNotificationToSQS(queue, payload2)

              eventually {
                assertQueueEmpty(queue)

                outgoing
                  .getMessages[EnrichedBagInformationPayload] should have size 2

                assertTopicReceivesIngestEvents(
                  payload1.ingestId,
                  ingests,
                  expectedDescriptions = Seq(
                    "Auditing bag started",
                    "Assigned bag version 1",
                    "Auditing bag succeeded",
                    "Auditing bag started",
                    "Assigned bag version 2",
                    "Auditing bag succeeded"
                  )
                )
              }
            }
          }
      }
    }
  }

  // TODO: When we pass an ingest type in the bag auditor payload, check it sends
  // an appropriate user-facing message in the ingests app.
}
