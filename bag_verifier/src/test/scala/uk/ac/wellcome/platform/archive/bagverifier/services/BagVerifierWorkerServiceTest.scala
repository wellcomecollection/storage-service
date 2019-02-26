package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

class BagVerifierWorkerServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with ProgressUpdateAssertions
    with WorkerServiceFixture {
  it(
    "updates the progress monitor and sends an ongoing notification if verification succeeds") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { ongoingTopic =>
        withWorkerService(progressTopic, ongoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket) { bagLocation =>
              val bagRequest = BagRequest(
                archiveRequestId = randomUUID,
                bagLocation = bagLocation
              )

              val future = service.processMessage(bagRequest)

              whenReady(future) { _ =>
                assertSnsReceivesOnly(bagRequest, topic = ongoingTopic)

                assertTopicReceivesProgressStatusUpdate(
                  requestId = bagRequest.archiveRequestId,
                  progressTopic = progressTopic,
                  status = Progress.Processing
                ) { events =>
                  events.map { _.description } shouldBe List(
                    "Successfully verified bag contents")
                }
              }
            }
          }
        }
      }
    }
  }

  it("only updates the progress monitor if verification fails") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { ongoingTopic =>
        withWorkerService(progressTopic, ongoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
              bagLocation =>
                val bagRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = bagLocation
                )

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  assertSnsReceivesNothing(ongoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.archiveRequestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map { _.description }.head
                    description should startWith(
                      "There were problems verifying the bag: not every checksum matched the manifest")
                  }
                }
            }
          }
        }
      }
    }
  }

  it("only updates the progress monitor if it cannot perform the verification") {
    def dontCreateTheDataManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { ongoingTopic =>
        withWorkerService(progressTopic, ongoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
              bagLocation =>
                val bagRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = bagLocation
                )

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  assertSnsReceivesNothing(ongoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.archiveRequestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map { _.description }.head
                    description should startWith(
                      "There were problems verifying the bag: verification could not be performed")
                  }
                }
            }
          }
        }
      }
    }
  }

  it("sends a progress update before it sends an ongoing message") {
    withLocalSnsTopic { progressTopic =>
      withWorkerService(progressTopic, Topic("no-such-ongoing")) { service =>
        withLocalS3Bucket { bucket =>
          withBag(bucket) { bagLocation =>
            val bagRequest = BagRequest(
              archiveRequestId = randomUUID,
              bagLocation = bagLocation
            )

            val future = service.processMessage(bagRequest)

            whenReady(future.failed) { _ =>
              assertTopicReceivesProgressStatusUpdate(
                requestId = bagRequest.archiveRequestId,
                progressTopic = progressTopic,
                status = Progress.Processing
              ) { events =>
                events.map {
                  _.description
                } shouldBe List("Successfully verified bag contents")
              }
            }
          }
        }
      }
    }
  }
}
