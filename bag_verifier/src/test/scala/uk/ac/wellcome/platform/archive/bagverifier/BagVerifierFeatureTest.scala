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
                    listMessagesReceivedFromSNS(outgoingTopic)

                    assertTopicReceivesProgressEventUpdate(
                      requestId = bagRequest.requestId,
                      progressTopic = progressTopic
                    ) { events =>
                      events.map {
                        _.description
                      } shouldBe List("Verification succeeded")
                    }

                    assertSnsReceivesOnly(bagRequest, topic = outgoingTopic)

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
                      assertTopicReceivesProgressStatusUpdate(
                        requestId = bagRequest.requestId,
                        progressTopic = progressTopic,
                        status = Progress.Failed
                      ) { events =>
                        val description = events.map {
                          _.description
                        }.head
                        description should startWith(
                          "Verification failed")
                      }

                      assertSnsReceivesNothing(outgoingTopic)

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
