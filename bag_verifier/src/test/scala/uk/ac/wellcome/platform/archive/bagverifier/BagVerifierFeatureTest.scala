package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class BagVerifierFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with IntegrationPatience
    with IngestUpdateAssertions
    with BagVerifierFixtures
    with PayloadGenerators {

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds") {
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withLocalSqsQueueAndDlq {
          case QueuePair(queue, dlq) =>
            withBagVerifierWorker(ingestTopic, outgoingTopic, queue) { _ =>
              withLocalS3Bucket { bucket =>
                withBag(bucket) {
                  case (bagRootLocation, _) =>
                    val payload = createBagInformationPayloadWith(
                      bagRootLocation = bagRootLocation
                    )

                    sendNotificationToSQS(queue, payload)

                    eventually {
                      listMessagesReceivedFromSNS(outgoingTopic)

                      assertTopicReceivesIngestEvents(
                        payload.ingestId,
                        ingestTopic,
                        expectedDescriptions = Seq(
                          "Verification started",
                          "Verification succeeded"
                        )
                      )

                      assertSnsReceivesOnly(payload, topic = outgoingTopic)

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
                  case (bagRootLocation, _) =>
                    val payload = createBagInformationPayloadWith(
                      bagRootLocation = bagRootLocation
                    )

                    sendNotificationToSQS(queue, payload)

                    eventually {
                      assertTopicReceivesIngestUpdates(
                        payload.ingestId,
                        ingestTopic) { ingestUpdates =>
                        ingestUpdates.size shouldBe 2

                        val ingestStart = ingestUpdates.head
                        ingestStart.events.head.description shouldBe "Verification started"

                        val ingestFailed =
                          ingestUpdates.tail.head
                            .asInstanceOf[IngestStatusUpdate]
                        ingestFailed.status shouldBe Ingest.Failed
                        ingestFailed.events.head.description shouldBe "Verification failed"
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
