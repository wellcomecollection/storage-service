package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestStatusUpdate}
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(storageBackend, namespace = bucket.name, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          val payload = createUnpackedBagPayloadWith(
            unpackedBagLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createBagInformationPayloadWith(
            ingestId = payload.ingestId,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          val ingests = createMessageSender
          val outgoing = createMessageSender

          withLocalSqsQueue { queue =>
            withAuditorWorker(queue, ingests, outgoing) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing.messages
                  .map { _.body }
                  .map { fromJson[BagInformationPayload](_).get } shouldBe Seq(expectedPayload)

                assertReceivesIngestEvents(ingests)(
                  payload.ingestId,
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
      withBag(storageBackend, namespace = bucket.name, bagInfo = bagInfo, bagRootDirectory = Some("subdir")) {
        case (unpackedBagLocation, storageSpace) =>
          val bagRootLocation = unpackedBagLocation.join("subdir")

          val payload = createUnpackedBagPayloadWith(
            unpackedBagLocation = unpackedBagLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createBagInformationPayloadWith(
            ingestId = payload.ingestId,
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          val ingests = createMessageSender
          val outgoing = createMessageSender

          withLocalSqsQueue { queue =>
            withAuditorWorker(queue, ingests, outgoing) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing.messages
                  .map { _.body }
                  .map { fromJson[BagInformationPayload](_).get } shouldBe Seq(expectedPayload)

                assertReceivesIngestEvents(ingests)(
                  payload.ingestId,
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
      withBag(storageBackend, namespace = bucket.name, bagRootDirectory = Some("subdir1/subdir2/subdir3")) {
        case (unpackedBagLocation, _) =>
          val payload = createUnpackedBagPayloadWith(unpackedBagLocation)

          val ingests = createMessageSender
          val outgoing = createMessageSender

          withLocalSqsQueue { queue =>
            withAuditorWorker(queue, ingests, outgoing) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                outgoing.messages shouldBe empty

                assertReceivesIngestUpdates(ingests)(
                  payload.ingestId) { ingestUpdates =>
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
      val payload = createUnpackedBagPayloadWith(unpackedBagLocation)

      val ingests = createMessageSender
      val outgoing = createMessageSender

      withLocalSqsQueue { queue =>
        withAuditorWorker(queue, ingests, outgoing) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertQueueEmpty(queue)

            outgoing.messages shouldBe empty

            assertReceivesIngestUpdates(ingests)(payload.ingestId) {
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
    val payload = createUnpackedBagPayloadWith(unpackedBagLocation)

    val ingests = createMessageSender
    val outgoing = createMessageSender

    withLocalSqsQueue { queue =>
      withAuditorWorker(queue, ingests, outgoing) { _ =>
        sendNotificationToSQS(queue, payload)

        eventually {
          assertQueueEmpty(queue)

          outgoing.messages shouldBe empty

          assertReceivesIngestUpdates(ingests)(payload.ingestId) {
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
