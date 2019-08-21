package uk.ac.wellcome.platform.storage.bag_versioner

import java.time.Instant

import org.scalatest.FunSpec
import org.scalatest.concurrent.Eventually
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion._
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  PayloadGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.storage.bag_versioner.fixtures.BagVersionerFixtures
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class BagVersionerFeatureTest
    extends FunSpec
    with BagVersionerFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ObjectLocationGenerators
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with Eventually {

  it("assigns a version for a new bag") {
    val bagRoot = createObjectLocationPrefix
    val storageSpace = createStorageSpace

    val payload = createBagRootLocationPayloadWith(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      bagRoot = bagRoot
    )

    val expectedPayload = createEnrichedBagInformationPayloadWith(
      context = payload.context,
      bagRoot = bagRoot,
      version = BagVersion(1)
    )

    withLocalSqsQueue { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()
      withAuditorWorker(queue, ingests, outgoing, stepName = "auditing bag") {
        _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing
              .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
              expectedPayload
            )

            assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates should have size 2

                ingestUpdates(0) shouldBe a[IngestEventUpdate]
                ingestUpdates(0).events.map { _.description } shouldBe Seq(
                  "Auditing bag started"
                )

                ingestUpdates(1) shouldBe a[IngestVersionUpdate]
                ingestUpdates(1)
                  .asInstanceOf[IngestVersionUpdate]
                  .version shouldBe BagVersion(1)
                ingestUpdates(1).events.map { _.description } shouldBe Seq(
                  "Auditing bag succeeded - assigned bag version v1"
                )
            }
          }
      }
    }
  }

  it("assigns a version for an updated bag") {
    val storageSpace = createStorageSpace

    val payload1 = createBagRootLocationPayloadWith(
      context = createPipelineContextWith(
        ingestId = createIngestID,
        ingestType = CreateIngestType,
        ingestDate = Instant.ofEpochSecond(1),
        storageSpace = storageSpace
      )
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
      withAuditorWorker(queue, ingests, outgoing, stepName = "auditing bag") {
        _ =>
          // Send the initial payload with "create" and check it completes
          sendNotificationToSQS(queue, payload1)

          eventually {
            assertQueueEmpty(queue)

            outgoing
              .getMessages[EnrichedBagInformationPayload] should have size 1

            assertTopicReceivesIngestUpdates(payload1.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates should have size 2

                ingestUpdates(0) shouldBe a[IngestEventUpdate]
                ingestUpdates(0).events.map { _.description } shouldBe Seq(
                  "Auditing bag started"
                )

                ingestUpdates(1) shouldBe a[IngestVersionUpdate]
                ingestUpdates(1)
                  .asInstanceOf[IngestVersionUpdate]
                  .version shouldBe BagVersion(1)
                ingestUpdates(1).events.map { _.description } shouldBe Seq(
                  "Auditing bag succeeded - assigned bag version v1"
                )
            }
          }

          // Now send the payload with "update"
          sendNotificationToSQS(queue, payload2)

          eventually {
            assertQueueEmpty(queue)

            outgoing
              .getMessages[EnrichedBagInformationPayload] should have size 2

            assertTopicReceivesIngestUpdates(payload1.ingestId, ingests) {
              ingestUpdates =>
                ingestUpdates should have size 4

                ingestUpdates(0) shouldBe a[IngestEventUpdate]
                ingestUpdates(0).events.map { _.description } shouldBe Seq(
                  "Auditing bag started"
                )

                ingestUpdates(1) shouldBe a[IngestVersionUpdate]
                ingestUpdates(1)
                  .asInstanceOf[IngestVersionUpdate]
                  .version shouldBe BagVersion(1)
                ingestUpdates(1).events.map { _.description } shouldBe Seq(
                  "Auditing bag succeeded - assigned bag version v1"
                )

                ingestUpdates(2) shouldBe a[IngestEventUpdate]
                ingestUpdates(2).events.map { _.description } shouldBe Seq(
                  "Auditing bag started"
                )

                ingestUpdates(3) shouldBe a[IngestVersionUpdate]
                ingestUpdates(3)
                  .asInstanceOf[IngestVersionUpdate]
                  .version shouldBe BagVersion(2)
                ingestUpdates(3).events.map { _.description } shouldBe Seq(
                  "Auditing bag succeeded - assigned bag version v2"
                )
            }
          }
      }
    }
  }
}
