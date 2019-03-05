package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
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
    with IntegrationPatience
    with WorkerServiceFixture {

  it(
    "updates the progress monitor and sends an outgoing notification if verification succeeds") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withWorkerService(progressTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket) { bagLocation =>
              val bagRequest = BagRequest(
                requestId = randomUUID,
                bagLocation = bagLocation
              )

              val future = service.processMessage(bagRequest)

              whenReady(future) { _ =>
                assertSnsReceivesOnly(bagRequest, topic = outgoingTopic)

                assertTopicReceivesProgressEventUpdate(
                  requestId = bagRequest.requestId,
                  progressTopic = progressTopic
                ) { events =>
                  events.map {
                    _.description
                  } shouldBe List(
                    "Successfully verified bag contents"
                  )
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
      withLocalSnsTopic { outgoingTopic =>
        withWorkerService(progressTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
              bagLocation =>
                val bagRequest = BagRequest(
                  requestId = randomUUID,
                  bagLocation = bagLocation
                )

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  assertSnsReceivesNothing(outgoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.requestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map {
                      _.description
                    }.head
                    description should startWith(
                      "Problem verifying bag: File checksum did not match manifest")
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
      withLocalSnsTopic { outgoingTopic =>
        withWorkerService(progressTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
              bagLocation =>
                val bagRequest = BagRequest(
                  requestId = randomUUID,
                  bagLocation = bagLocation
                )

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  assertSnsReceivesNothing(outgoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.requestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map {
                      _.description
                    }.head
                    description should startWith(
                      "Problem verifying bag: Verification could not be performed")
                  }
                }
            }
          }
        }
      }
    }
  }

  it("sends a progress update before it sends an outgoing message") {
    withLocalSnsTopic { progressTopic =>
      withWorkerService(progressTopic, Topic("no-such-outgoing")) { service =>
        withLocalS3Bucket { bucket =>
          withBag(bucket) { bagLocation =>
            val bagRequest = BagRequest(
              requestId = randomUUID,
              bagLocation = bagLocation
            )

            val future = service.processMessage(bagRequest)

            whenReady(future.failed) { _ =>
              assertTopicReceivesProgressEventUpdate(
                requestId = bagRequest.requestId,
                progressTopic = progressTopic
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
