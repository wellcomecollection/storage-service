package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.{
  BagRootPayload,
  EnrichedBagInformationPayload
}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}

class BagVerifierFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with BagLocationFixtures
    with IntegrationPatience
    with IngestUpdateAssertions
    with BagVerifierFixtures
    with PayloadGenerators {

  it(
    "updates the ingests service and sends an outgoing notification if verification succeeds") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalSqsQueueAndDlq {
      case QueuePair(queue, dlq) =>
        withBagVerifierWorker(
          ingests,
          outgoing,
          queue,
          stepName = "verification") { _ =>
          withLocalS3Bucket { bucket =>
            withBag(bucket) {
              case (bagRootLocation, _) =>
                val payload = createEnrichedBagInformationPayload(
                  bagRootLocation = bagRootLocation
                )

                sendNotificationToSQS[BagRootPayload](queue, payload)

                eventually {
                  assertTopicReceivesIngestEvents(
                    payload.ingestId,
                    ingests,
                    expectedDescriptions = Seq(
                      "Verification started",
                      "Verification succeeded"
                    )
                  )

                  outgoing
                    .getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                    payload)

                  assertQueueEmpty(queue)
                  assertQueueEmpty(dlq)
                }
            }
          }
        }
    }
  }

  it("deletes the message if the bag has incorrect checksum values") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalSqsQueueAndDlq {
      case QueuePair(queue, dlq) =>
        withBagVerifierWorker(
          ingests,
          outgoing,
          queue,
          stepName = "verification") { _ =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
              case (bagRootLocation, _) =>
                val payload = createEnrichedBagInformationPayload(
                  bagRootLocation = bagRootLocation
                )

                sendNotificationToSQS[BagRootPayload](queue, payload)

                eventually {
                  assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
                    ingestUpdates =>
                      debug(s"Got $ingestUpdates")

                      ingestUpdates.size shouldBe 2

                      val ingestStart = ingestUpdates.head
                      ingestStart.events.head.description shouldBe "Verification started"

                      val ingestFailed =
                        ingestUpdates.tail.head
                          .asInstanceOf[IngestStatusUpdate]
                      ingestFailed.status shouldBe Ingest.Failed
                      ingestFailed.events.head.description shouldBe "Verification failed"
                  }

                  outgoing.messages shouldBe empty

                  assertQueueEmpty(queue)
                  assertQueueEmpty(dlq)
                }
            }
          }
        }
    }
  }
}
