package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
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

          withLocalSqsQueue { queue =>
            withLocalSnsTopic { ingestTopic =>
              withLocalSnsTopic { outgoingTopic =>
                withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
                  sendNotificationToSQS(queue, payload)

                  eventually {
                    assertQueueEmpty(queue)

                    assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                    assertTopicReceivesIngestEvents(
                      payload.ingestId,
                      ingestTopic,
                      expectedDescriptions = Seq(
                        "Locating bag root started",
                        "Locating bag root succeeded"
                      )
                    )
                  }
                }
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
          val bagRoot = unpackedBagLocation.join("subdir")

          val payload = createUnpackedBagPayloadWith(
            unpackedBagLocation = unpackedBagLocation,
            storageSpace = storageSpace
          )

          val expectedPayload = createBagInformationPayloadWith(
            ingestId = payload.ingestId,
            bagRootLocation = bagRoot,
            storageSpace = storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          withLocalSqsQueue { queue =>
            withLocalSnsTopic { ingestTopic =>
              withLocalSnsTopic { outgoingTopic =>
                withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
                  sendNotificationToSQS(queue, payload)

                  eventually {
                    assertQueueEmpty(queue)

                    assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                    assertTopicReceivesIngestEvents(
                      payload.ingestId,
                      ingestTopic,
                      expectedDescriptions = Seq(
                        "Locating bag root started",
                        "Locating bag root succeeded"
                      )
                    )
                  }
                }
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
          val payload = createUnpackedBagPayloadWith(unpackedBagLocation)

          withLocalSqsQueue { queue =>
            withLocalSnsTopic { ingestTopic =>
              withLocalSnsTopic { outgoingTopic =>
                withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
                  sendNotificationToSQS(queue, payload)

                  eventually {
                    assertQueueEmpty(queue)

                    assertSnsReceivesNothing(outgoingTopic)

                    assertTopicReceivesIngestUpdates(
                      payload.ingestId,
                      ingestTopic) { ingestUpdates =>
                      ingestUpdates.size shouldBe 2

                      val ingestStart = ingestUpdates.head
                      ingestStart.events.head.description shouldBe "Locating bag root started"

                      val ingestFailed =
                        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                      ingestFailed.status shouldBe Ingest.Failed
                      ingestFailed.events.head.description shouldBe "Locating bag root failed"
                    }
                  }
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

      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                assertSnsReceivesNothing(outgoingTopic)

                assertTopicReceivesIngestUpdates(payload.ingestId, ingestTopic) {
                  ingestUpdates =>
                    ingestUpdates.size shouldBe 2

                    val ingestStart = ingestUpdates.head
                    ingestStart.events.head.description shouldBe "Locating bag root started"

                    val ingestFailed =
                      ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                    ingestFailed.status shouldBe Ingest.Failed
                    ingestFailed.events.head.description shouldBe "Locating bag root failed"
                }
              }
            }
          }
        }
      }
    }
  }

  it("errors if it gets an error from S3") {
    val unpackedBagLocation = createObjectLocation
    val payload = createUnpackedBagPayloadWith(unpackedBagLocation)

    withLocalSqsQueue { queue =>
      withLocalSnsTopic { ingestTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              assertQueueEmpty(queue)

              assertSnsReceivesNothing(outgoingTopic)

              assertTopicReceivesIngestUpdates(payload.ingestId, ingestTopic) {
                ingestUpdates =>
                  ingestUpdates.size shouldBe 2

                  val ingestStart = ingestUpdates.head
                  ingestStart.events.head.description shouldBe "Locating bag root started"

                  val ingestFailed =
                    ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
                  ingestFailed.status shouldBe Ingest.Failed
                  ingestFailed.events.head.description shouldBe "Locating bag root failed"
              }
            }
          }
        }
      }
    }
  }
}
