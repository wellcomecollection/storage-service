package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestStatusUpdate}

class BagVerifierFeatureTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
    with IngestUpdateAssertions
    with BagVerifierFixtures
    with PayloadGenerators {

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds") {
    val ingests = createMessageSender
    val outgoing = createMessageSender

    withLocalSqsQueueAndDlq {
      case QueuePair(queue, dlq) =>
        withBagVerifierWorker(ingests, outgoing, queue) { _ =>
          withLocalS3Bucket { bucket =>
            withBag(storageBackend, namespace = bucket.name) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                sendNotificationToSQS(queue, payload)

                eventually {
                  assertReceivesIngestEvents(ingests)(
                    payload.ingestId,
                    expectedDescriptions = Seq(
                      "Verification started",
                      "Verification succeeded"
                    )
                  )

                  outgoing.messages
                    .map { _.body }
                    .map { fromJson[BagInformationPayload](_).get } shouldBe Seq(payload)

                  assertQueueEmpty(queue)
                  assertQueueEmpty(dlq)
                }
            }
          }
        }
    }
  }

  it(
    "deletes the SQS message if the bag can be verified but has incorrect checksums") {
    val ingests = createMessageSender
    val outgoing = createMessageSender


    withLocalSqsQueueAndDlq {
      case QueuePair(queue, dlq) =>
        withBagVerifierWorker(ingests, outgoing, queue) { _ =>
          withLocalS3Bucket { bucket =>
            withBag(
              storageBackend,
              namespace = bucket.name,
              createDataManifest = dataManifestWithWrongChecksum) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                sendNotificationToSQS(queue, payload)

                eventually {
                  assertReceivesIngestUpdates(ingests)(
                    payload.ingestId) { ingestUpdates =>
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
