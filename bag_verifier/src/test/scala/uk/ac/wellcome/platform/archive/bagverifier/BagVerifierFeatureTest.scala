package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.PayloadEntry
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  VerifiablePayload
}

class BagVerifierFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with IngestUpdateAssertions
    with BagVerifierFixtures
    with PayloadGenerators
    with S3BagBuilder {

  it(
    "updates the ingests service and sends an outgoing notification if verification succeeds"
  ) {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    // The default visibility timeout is 1 second, which turns out to be a little short
    // depending on the size of the bag being verified.
    //
    // If the verification doesn't succeed within 1 second, it gets retried 3 times until
    // SQS sends the message to the DLQ, at which point the test fails.
    //
    // This was a cause of significant flakiness!  You can see the issue by turning down
    // the timeout and turning up the payload file count in `createS3BagWith()`.
    withLocalSqsQueuePair(visibilityTimeout = 10) {
      case QueuePair(queue, dlq) =>
        withLocalS3Bucket { bucket =>
          withStandaloneBagVerifierWorker(
            ingests,
            outgoing,
            queue,
            bucket = bucket,
            stepName = "verification"
          ) { _ =>
            val space = createStorageSpace
            val (bagRoot, bagInfo) = storeBagWith(space = space)(
              namespace = bucket,
              primaryBucket = bucket
            )

            val payload = createBagRootLocationPayloadWith(
              context = createPipelineContextWith(
                externalIdentifier = bagInfo.externalIdentifier,
                storageSpace = space
              ),
              bagRoot = bagRoot
            )

            sendNotificationToSQS[VerifiablePayload](queue, payload)

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
                .getMessages[BagRootLocationPayload]
                .toSet shouldBe Set(payload)

              assertQueueEmpty(queue)
              assertQueueEmpty(dlq)
            }
          }
        }
    }
  }

  it("deletes the message if the bag has the wrong Payload-Oxum") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalSqsQueuePair() {
      case QueuePair(queue, dlq) =>
        withLocalS3Bucket { bucket =>
          withStandaloneBagVerifierWorker(
            ingests,
            outgoing,
            queue,
            bucket = bucket,
            stepName = "verification"
          ) { _ =>
            val badBuilder: S3BagBuilder = new S3BagBuilder {
              override protected def createPayloadManifest(
                entries: Seq[PayloadEntry]
              ): Option[String] =
                super
                  .createPayloadManifest(entries)
                  .map { _ + "\nbad123  badName" }
            }

            val (bagRootLocation, bagInfo) = badBuilder
              .storeBagWith()(namespace = bucket, primaryBucket = bucket)

            val payload = createVersionedBagRootPayloadWith(
              context = createPipelineContextWith(
                externalIdentifier = bagInfo.externalIdentifier
              ),
              bagRoot = bagRootLocation
            )

            sendNotificationToSQS[VerifiablePayload](queue, payload)

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
                  ingestFailed.events.head.description should startWith(
                    "Verification failed - Payload-Oxum has the wrong number of payload files"
                  )
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
