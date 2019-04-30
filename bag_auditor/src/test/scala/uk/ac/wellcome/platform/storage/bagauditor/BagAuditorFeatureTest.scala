package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(
        bucket,
        "bag123/bag-info.txt",
        "bag123/data/1.jpg",
        "bag123/data/2.jpg"
      )

      val searchRoot = createObjectLocationWith(bucket, "bag123")
      val payload = createObjectLocationPayloadWith(searchRoot)
      val expectedPayload = payload

      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                assertTopicReceivesIngestEvent(payload.ingestId, ingestTopic) {
                  events =>
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

  it("detects a bag in a subdirectory of the bagLocation") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(
        bucket,
        "bag123/subdir/bag-info.txt",
        "bag123/subdir/data/1.jpg",
        "bag123/subdir/data/2.jpg"
      )

      val searchRoot = createObjectLocationWith(bucket, "bag123")
      val bagRoot = createObjectLocationWith(bucket, "bag123/subdir")

      val payload = createObjectLocationPayloadWith(searchRoot)
      val expectedPayload = payload.copy(objectLocation = bagRoot)

      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
              sendNotificationToSQS(queue, payload)

              eventually {
                assertQueueEmpty(queue)

                assertSnsReceivesOnly(expectedPayload, outgoingTopic)

                assertTopicReceivesIngestEvent(payload.ingestId, ingestTopic) {
                  events =>
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

  it("errors if the bag is nested too deep") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(
        bucket,
        "bag123/subdir1/subdir2/subdir3/bag-info.txt",
        "bag123/subdir1/subdir2/subdir3/data/1.jpg",
        "bag123/subdir1/subdir2/subdir3/data/2.jpg"
      )

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

  def createObjectsWith(bucket: Bucket, keys: String*): Unit =
    keys.foreach { k =>
      s3Client.putObject(bucket.name, k, "example object")
    }
}
