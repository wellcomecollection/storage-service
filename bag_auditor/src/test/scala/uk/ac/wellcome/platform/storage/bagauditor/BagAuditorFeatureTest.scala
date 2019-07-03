package uk.ac.wellcome.platform.storage.bagauditor

import java.time.Instant

import org.scalatest.FunSpec
import org.scalatest.concurrent.Eventually
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, PayloadGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, UpdateIngestType}
import uk.ac.wellcome.platform.archive.common.{BagRootLocationPayload, EnrichedBagInformationPayload}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ObjectLocationGenerators
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with Eventually {

  it("assigns a version for a new bag") {
    val bagRootLocation = createObjectLocation
    val storageSpace = createStorageSpace

    val payload = createBagRootLocationPayloadWith(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      bagRootLocation = bagRootLocation
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

  it("assigns a version for an updated bag") {
    val bagRootLocation = createObjectLocation
    val storageSpace = createStorageSpace

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

  // TODO: When we pass an ingest type in the bag auditor payload, check it sends
  // an appropriate user-facing message in the ingests app.
}
