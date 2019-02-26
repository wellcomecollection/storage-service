package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

class BagVerifierFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with IntegrationPatience
    with ProgressUpdateAssertions
    with WorkerServiceFixture {

  it("updates the progress monitor and sends an ongoing notification if verification succeeds") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { ongoingTopic =>
        withLocalSqsQueueAndDlq { case QueuePair(queue, dlq) =>
          withWorkerService(progressTopic, ongoingTopic, queue) { service =>
            withLocalS3Bucket { bucket =>
              withBag(bucket) { bagLocation =>
                val bagRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = bagLocation
                )

                sendNotificationToSQS(queue, bagRequest)

                eventually {
                  assertSnsReceivesOnly(bagRequest, topic = ongoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.archiveRequestId,
                    progressTopic = progressTopic,
                    status = Progress.Processing
                  ) { events =>
                    events.map {
                      _.description
                    } shouldBe List("Successfully verified bag contents")
                  }

                  assertQueueEmpty(queue)
                  assertQueueEmpty(dlq)
                }
              }
            }
          }
        }
      }
    }
  }

  it("deletes the SQS message if the bag can be verified but has incorrect checksums") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { ongoingTopic =>
        withLocalSqsQueueAndDlq { case QueuePair(queue, dlq) =>
          withWorkerService(progressTopic, ongoingTopic, queue) { _ =>
            withLocalS3Bucket { bucket =>
              withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) { bagLocation =>
                val bagRequest = BagRequest(
                  archiveRequestId = randomUUID,
                  bagLocation = bagLocation
                )

                sendNotificationToSQS(queue, bagRequest)

                eventually {
                  assertSnsReceivesNothing(ongoingTopic)

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = bagRequest.archiveRequestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map {
                      _.description
                    }.head
                    description should startWith("There were problems verifying the bag: not every checksum matched the manifest")
                  }

                  assertQueueEmpty(queue)
                  assertQueueEmpty(dlq)
                }
              }
            }
          }
        }
      }
    }
  }
}
