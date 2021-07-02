package weco.storage_service.bag_versioner

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.bagit.models.BagVersion._
import weco.storage_service.generators.{
  ExternalIdentifierGenerators,
  PayloadGenerators,
  StorageSpaceGenerators
}
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.ingests.models._
import weco.storage_service.VersionedBagRootPayload
import weco.storage_service.bag_versioner.fixtures.BagVersionerFixtures

class BagVersionerFeatureTest
    extends AnyFunSpec
    with BagVersionerFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with Eventually
    with IntegrationPatience {

  it("assigns a version for a new bag") {
    val bagRoot = createS3ObjectLocationPrefix
    val storageSpace = createStorageSpace

    val payload = createBagRootLocationPayloadWith(
      context = createPipelineContextWith(
        storageSpace = storageSpace,
        ingestType = CreateIngestType
      ),
      bagRoot = bagRoot
    )

    val expectedPayload = createVersionedBagRootPayloadWith(
      context = payload.context,
      bagRoot = bagRoot,
      version = BagVersion(1)
    )

    withLocalSqsQueue() { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()
      withBagVersionerWorker(
        queue,
        ingests,
        outgoing,
        stepName = "assigning bag version"
      ) { _ =>
        sendNotificationToSQS(queue, payload)

        eventually {
          assertQueueEmpty(queue)

          outgoing
            .getMessages[VersionedBagRootPayload] shouldBe Seq(
            expectedPayload
          )

          assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
            ingestUpdates should have size 2

            ingestUpdates(0) shouldBe a[IngestEventUpdate]
            ingestUpdates(0).events.map { _.description } shouldBe Seq(
              "Assigning bag version started"
            )

            ingestUpdates(1) shouldBe a[IngestVersionUpdate]
            ingestUpdates(1)
              .asInstanceOf[IngestVersionUpdate]
              .version shouldBe BagVersion(1)
            ingestUpdates(1).events.map { _.description } shouldBe Seq(
              "Assigning bag version succeeded - assigned bag version v1"
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

    withLocalSqsQueue() { queue =>
      withBagVersionerWorker(
        queue,
        ingests,
        outgoing,
        stepName = "assigning bag version"
      ) { _ =>
        // Send the initial payload with "create" and check it completes
        sendNotificationToSQS(queue, payload1)

        eventually {
          assertQueueEmpty(queue)

          outgoing
            .getMessages[VersionedBagRootPayload] should have size 1

          assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
            ingestUpdates should have size 2

            ingestUpdates(0) shouldBe a[IngestEventUpdate]
            ingestUpdates(0).events.map { _.description } shouldBe Seq(
              "Assigning bag version started"
            )

            ingestUpdates(1) shouldBe a[IngestVersionUpdate]
            ingestUpdates(1)
              .asInstanceOf[IngestVersionUpdate]
              .version shouldBe BagVersion(1)
            ingestUpdates(1).events.map { _.description } shouldBe Seq(
              "Assigning bag version succeeded - assigned bag version v1"
            )
          }
        }

        // Now send the payload with "update"
        sendNotificationToSQS(queue, payload2)

        eventually {
          assertQueueEmpty(queue)

          outgoing
            .getMessages[VersionedBagRootPayload] should have size 2

          assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
            ingestUpdates should have size 4

            ingestUpdates(0) shouldBe a[IngestEventUpdate]
            ingestUpdates(0).events.map { _.description } shouldBe Seq(
              "Assigning bag version started"
            )

            ingestUpdates(1) shouldBe a[IngestVersionUpdate]
            ingestUpdates(1)
              .asInstanceOf[IngestVersionUpdate]
              .version shouldBe BagVersion(1)
            ingestUpdates(1).events.map { _.description } shouldBe Seq(
              "Assigning bag version succeeded - assigned bag version v1"
            )

            ingestUpdates(2) shouldBe a[IngestEventUpdate]
            ingestUpdates(2).events.map { _.description } shouldBe Seq(
              "Assigning bag version started"
            )

            ingestUpdates(3) shouldBe a[IngestVersionUpdate]
            ingestUpdates(3)
              .asInstanceOf[IngestVersionUpdate]
              .version shouldBe BagVersion(2)
            ingestUpdates(3).events.map { _.description } shouldBe Seq(
              "Assigning bag version succeeded - assigned bag version v2"
            )
          }
        }
      }
    }
  }
}
