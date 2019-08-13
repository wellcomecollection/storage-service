package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths
import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.{
  S3BagBuilder,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestShouldRetry,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.locking.{LockDao, LockFailure}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.transfer.s3.{S3PrefixTransfer, S3Transfer}
import uk.ac.wellcome.storage.transfer.{
  TransferFailure,
  TransferPerformed,
  TransferSuccess
}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ScalaFutures {

  it("replicates a bag successfully and updates both topics") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
        bucket = srcBucket
      )

      val payload = createEnrichedBagInformationPayloadWith(
        bagRootLocation = srcBagLocation
      )

      withLocalS3Bucket { dstBucket =>
        val future =
          withBagReplicatorWorker(
            bucket = dstBucket,
            ingests = ingests,
            outgoing = outgoing,
            stepName = "replicating"
          ) {
            _.processPayload(payload)
          }

        val serviceResult = whenReady(future) { result =>
          result
        }

        serviceResult shouldBe a[IngestStepSucceeded[_]]

        val receivedMessages =
          outgoing.getMessages[EnrichedBagInformationPayload]

        receivedMessages.size shouldBe 1

        val result = receivedMessages.head
        result.ingestId shouldBe payload.ingestId

        val dstBagLocation = result.bagRootLocation

        verifyObjectsCopied(
          src = srcBagLocation,
          dst = dstBagLocation
        )

        assertTopicReceivesIngestEvents(
          payload.ingestId,
          ingests,
          expectedDescriptions = Seq(
            "Replicating started",
            "Replicating succeeded"
          )
        )
      }
    }
  }

  describe("copies to the correct destination") {
    it("copies the bag to the configured bucket") {
      withLocalS3Bucket { srcBucket =>
        val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagLocation
        )

        withLocalS3Bucket { dstBucket =>
          val future =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processPayload(payload)
            }

          whenReady(future) { serviceResult =>
            serviceResult shouldBe a[IngestStepSucceeded[_]]

            val destination = serviceResult.summary.dstPrefix
            destination.namespace shouldBe dstBucket.name
          }

        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
            bucket = srcBucket
          )

          val payload = createEnrichedBagInformationPayloadWith(
            bagRootLocation = srcBagLocation
          )

          val future =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processPayload(payload)
            }

          whenReady(future) { stepResult =>
            stepResult shouldBe a[IngestStepSucceeded[_]]

            val dstBagLocation = stepResult.summary.dstPrefix
            val expectedPath =
              Paths
                .get(
                  payload.storageSpace.underlying,
                  payload.externalIdentifier.toString,
                  payload.version.toString
                )
                .toString
            dstBagLocation.path shouldBe expectedPath
          }
        }
      }
    }
  }

  it("only allows one worker to process a destination") {
    withLocalS3Bucket { srcBucket =>
      // We have to create enough files in the bag to keep the first
      // replicator busy, otherwise it completes and unlocks, and the
      // last replicator to start runs successfully.
      //
      // If this test becomes flaky, try increasing the payloadFileCount.
      val bagBuilder = new S3BagBuilderBase {
        override def getFetchEntryCount(payloadFileCount: Int): Int = 0
      }

      val (srcBagLocation, _) = bagBuilder.createS3BagWith(
        bucket = srcBucket,
        payloadFileCount = 50
      )

      val payload = createEnrichedBagInformationPayloadWith(
        bagRootLocation = srcBagLocation
      )

      withLocalS3Bucket { dstBucket =>
        withBagReplicatorWorker(bucket = dstBucket) { worker =>
          val futures: Future[Seq[IngestStepResult[ReplicationSummary]]] =
            Future.sequence(
              (1 to 5).map { _ =>
                worker.processPayload(payload)
              }
            )

          whenReady(futures) { result =>
            result.count { _.isInstanceOf[IngestStepSucceeded[_]] } shouldBe 1
            result.count { _.isInstanceOf[IngestShouldRetry[_]] } shouldBe 4
          }
        }
      }
    }
  }

  it("doesn't delete the SQS message if it can't acquire a lock") {
    val neverAllowLockDao = new LockDao[String, UUID] {
      override def lock(id: String, contextId: UUID): LockResult =
        Left(LockFailure(id, new Throwable("BOOM!")))

      override def unlock(contextId: UUID): UnlockResult = Right(Unit)
    }

    withLocalS3Bucket { srcBucket =>
      val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
        bucket = srcBucket
      )

      val payload = createEnrichedBagInformationPayloadWith(
        bagRootLocation = srcBagLocation
      )

      withLocalSqsQueue { queue =>
        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            queue = queue,
            bucket = dstBucket,
            lockServiceDao = neverAllowLockDao
          ) { _ =>
            sendNotificationToSQS(queue, payload)

            // Give the worker time to pick up the message, discover it
            // can't lock, and mark the message visibility timeout.
            Thread.sleep(2000)

            eventually {
              val queueAttributes =
                sqsClient
                  .getQueueAttributes(
                    queue.url,
                    List(
                      "ApproximateNumberOfMessagesNotVisible",
                      "ApproximateNumberOfMessages"
                    ).asJava
                  )
                  .getAttributes

              queueAttributes.get("ApproximateNumberOfMessagesNotVisible") shouldBe "1"
              queueAttributes.get("ApproximateNumberOfMessages") shouldBe "0"
            }
          }
        }
      }
    }
  }

  describe("checks the tagmanifest-sha256.txt matches the original bag") {
    it("fails if the tag manifests don't match") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      val queue = Queue("any", "any")

      withLocalS3Bucket { srcBucket =>
        val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagLocation
        )

        withLocalS3Bucket { dstBucket =>
          withActorSystem { implicit actorSystem =>
            val ingestUpdater = createIngestUpdaterWith(ingests)
            val outgoingPublisher = createOutgoingPublisherWith(outgoing)
            withMonitoringClient { implicit monitoringClient =>
              val lockingService = createLockingService

              val replicatorDestinationConfig =
                createReplicatorDestinationConfigWith(dstBucket)

              implicit val badS3Transfer: S3Transfer =
                new S3Transfer() {
                  override def transfer(
                    src: ObjectLocation,
                    dst: ObjectLocation
                  ): Either[TransferFailure, TransferSuccess] =
                    if (dst.path.endsWith("/tagmanifest-sha256.txt")) {
                      s3Client.putObject(
                        dst.namespace,
                        dst.path,
                        "the wrong file contents"
                      )

                      Right(TransferPerformed(src, dst))
                    } else {
                      super.transfer(src, dst)
                    }
                }

              implicit val listing: S3ObjectLocationListing =
                S3ObjectLocationListing()

              implicit val prefixTransfer: S3PrefixTransfer =
                new S3PrefixTransfer()

              implicit val s3StreamStore: S3StreamStore =
                new S3StreamStore()

              val service = new BagReplicatorWorker(
                config = createAlpakkaSQSWorkerConfig(queue),
                bagReplicator = new BagReplicator(),
                ingestUpdater = ingestUpdater,
                outgoingPublisher = outgoingPublisher,
                lockingService = lockingService,
                replicatorDestinationConfig = replicatorDestinationConfig
              )

              val future = service.processPayload(payload)

              whenReady(future) { serviceResult =>
                serviceResult shouldBe a[IngestFailed[_]]

                val ingestFailed = serviceResult.asInstanceOf[IngestFailed[_]]
                ingestFailed.e.getMessage shouldBe "tagmanifest-sha256.txt in replica source and replica location do not match!"
              }
            }
          }
        }
      }
    }

    it("fails if there is no tag manifest") {
      val queue = Queue("any", "any")

      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
            bucket = srcBucket
          )

          val payload = createEnrichedBagInformationPayloadWith(
            bagRootLocation = srcBagLocation
          )

          s3Client.deleteObject(
            srcBagLocation.namespace,
            srcBagLocation.join("tagmanifest-sha256.txt").path
          )

          val future =
            withBagReplicatorWorker(queue, dstBucket) {
              _.processPayload(payload)
            }

          whenReady(future) { serviceResult =>
            serviceResult shouldBe a[IngestFailed[_]]

            val ingestFailed = serviceResult.asInstanceOf[IngestFailed[_]]
            ingestFailed.e.getMessage should startWith(
              "Unable to load tagmanifest-sha256.txt in source and replica to compare:"
            )
          }
        }
      }
    }
  }
}
