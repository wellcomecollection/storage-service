package uk.ac.wellcome.platform.storage.bag_versioner

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
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
import uk.ac.wellcome.platform.archive.common.VersionedBagRootPayload
import uk.ac.wellcome.platform.storage.bag_versioner.fixtures.BagVersionerFixtures
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class BagVersionerFeatureTest
    extends AnyFunSpec
    with BagVersionerFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ObjectLocationGenerators
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with Eventually
    with IntegrationPatience
    with NewS3Fixtures {

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

    withLocalSqsQueue(visibilityTimeout = 5) { queue =>
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

          assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
            ingestUpdates =>
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

          assertTopicReceivesIngestUpdates(payload1.ingestId, ingests) {
            ingestUpdates =>
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

          assertTopicReceivesIngestUpdates(payload1.ingestId, ingests) {
            ingestUpdates =>
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
