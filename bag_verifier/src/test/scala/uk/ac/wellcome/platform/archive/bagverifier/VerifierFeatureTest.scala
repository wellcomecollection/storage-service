package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class VerifierFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagRequestGenerators
    with IntegrationPatience
    with IngestUpdateAssertions
    with BagVerifierFixtures {

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds") {
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withLocalSqsQueueAndDlq {
          case QueuePair(queue, dlq) =>
            withBagVerifierWorker(ingestTopic, outgoingTopic, queue) { _ =>
              withLocalS3Bucket { bucket =>
                withBag(bucket) { bagLocation =>
                  val bagRequest = createBagRequestWith(bagLocation)

                  sendNotificationToSQS(queue, bagRequest)

                  eventually {
                    listMessagesReceivedFromSNS(outgoingTopic)

                    assertTopicReceivesIngestEvent(
                      requestId = bagRequest.ingestId,
                      ingestTopic = ingestTopic
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
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withLocalSqsQueueAndDlq {
          case QueuePair(queue, dlq) =>
            withBagVerifierWorker(ingestTopic, outgoingTopic, queue) { _ =>
              withLocalS3Bucket { bucket =>
                withBag(
                  bucket,
                  createDataManifest = dataManifestWithWrongChecksum) {
                  bagLocation =>
                    val bagRequest = createBagRequestWith(bagLocation)

                    sendNotificationToSQS(queue, bagRequest)

                    eventually {
                      assertTopicReceivesIngestStatus(
                        requestId = bagRequest.ingestId,
                        ingestTopic = ingestTopic,
                        status = Ingest.Failed
                      ) { events =>
                        val description = events.map {
                          _.description
                        }.head
                        description should startWith("Verification failed")
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
