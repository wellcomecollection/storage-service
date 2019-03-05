package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

class BagVerifierFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagRequestGenerators
    with IntegrationPatience
    with ProgressUpdateAssertions
    with WorkerServiceFixture {

  it(
    "updates the progress monitor and sends an outgoing notification if verification succeeds") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withLocalSqsQueueAndDlq {
          case QueuePair(queue, dlq) =>
            withWorkerService(progressTopic, outgoingTopic, queue) { _ =>
              withLocalS3Bucket { bucket =>
                withBag(bucket) { bagLocation =>
                  val bagRequest = createBagRequestWith(bagLocation)

                  sendNotificationToSQS(queue, bagRequest)

                  eventually {
                    assertSnsReceivesOnly(bagRequest, topic = outgoingTopic)

                    assertTopicReceivesProgressEventUpdate(
                      requestId = bagRequest.archiveRequestId,
                      progressTopic = progressTopic
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

  it(
    "deletes the SQS message if the bag can be verified but has incorrect checksums") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withLocalSqsQueueAndDlq {
          case QueuePair(queue, dlq) =>
            withWorkerService(progressTopic, outgoingTopic, queue) { _ =>
              withLocalS3Bucket { bucket =>
                withBag(
                  bucket,
                  createDataManifest = dataManifestWithWrongChecksum) {
                  bagLocation =>
                    val bagRequest = createBagRequestWith(bagLocation)

                    sendNotificationToSQS(queue, bagRequest)

                    eventually {
                      assertSnsReceivesNothing(outgoingTopic)

                      assertTopicReceivesProgressStatusUpdate(
                        requestId = bagRequest.archiveRequestId,
                        progressTopic = progressTopic,
                        status = Progress.Failed
                      ) { events =>
                        val description = events.map {
                          _.description
                        }.head
                        description should startWith(
                          "Problem verifying bag: File checksum did not match manifest")
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
