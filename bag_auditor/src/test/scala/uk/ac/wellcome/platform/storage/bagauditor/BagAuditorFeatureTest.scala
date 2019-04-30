package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
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
          val payload = createObjectLocationPayloadWith(
            objectLocation = bagRootLocation,
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

                    assertTopicReceivesIngestEvent(
                      payload.ingestId,
                      ingestTopic) { events =>
                      events should have size 1
                      events.head.description shouldBe "Locating bag root succeeded"
                    }
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
        case (searchRootLocation, storageSpace) =>
          val bagRoot = searchRootLocation.join("subdir")

          val payload = createObjectLocationPayloadWith(
            objectLocation = searchRootLocation,
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

                    assertTopicReceivesIngestEvent(
                      payload.ingestId,
                      ingestTopic) { events =>
                      events should have size 1
                      events.head.description shouldBe "Locating bag root succeeded"
                    }
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
        case (searchRootLocation, _) =>
          val payload = createObjectLocationPayloadWith(searchRootLocation)

          withLocalSqsQueue { queue =>
            withLocalSnsTopic { ingestTopic =>
              withLocalSnsTopic { outgoingTopic =>
                withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
                  sendNotificationToSQS(queue, payload)

                  eventually {
                    assertQueueEmpty(queue)

                    assertSnsReceivesNothing(outgoingTopic)

                    assertTopicReceivesIngestStatus(
                      payload.ingestId,
                      status = Ingest.Failed,
                      ingestTopic = ingestTopic) { events =>
                      events should have size 1
                      events.head.description shouldBe "Locating bag root failed"
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
      val searchRoot = createObjectLocationWith(bucket, "bag123")
      val payload = createObjectLocationPayloadWith(searchRoot)

      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                assertSnsReceivesNothing(outgoingTopic)

                assertTopicReceivesIngestStatus(
                  payload.ingestId,
                  status = Ingest.Failed,
                  ingestTopic = ingestTopic) { events =>
                  events should have size 1
                  events.head.description shouldBe "Locating bag root failed"
                }
              }
            }
          }
        }
      }
    }
  }

  it("errors if it gets an error from S3") {
    val searchRoot = createObjectLocationWith(key = "bag123")
    val payload = createObjectLocationPayloadWith(searchRoot)

    withLocalSqsQueue { queue =>
      withLocalSnsTopic { ingestTopic =>
        withLocalSnsTopic { outgoingTopic =>
          withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              assertQueueEmpty(queue)

              assertSnsReceivesNothing(outgoingTopic)

              assertTopicReceivesIngestStatus(
                payload.ingestId,
                status = Ingest.Failed,
                ingestTopic = ingestTopic) { events =>
                events should have size 1
                events.head.description shouldBe "Locating bag root failed"
              }
            }
          }
        }
      }
    }
  }
}
