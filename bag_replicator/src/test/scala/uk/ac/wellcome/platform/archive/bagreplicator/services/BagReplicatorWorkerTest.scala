package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths
import java.util.UUID

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.bags.BagReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models.{
  PrimaryBagReplicationRequest,
  SecondaryBagReplicationRequest
}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.common.ReplicaResultPayload
import uk.ac.wellcome.platform.archive.common.fixtures.{
  S3BagBuilder,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.locking.memory.MemoryLockDao
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
    with ScalaFutures
    with IntegrationPatience
    with TryValues {

  it("replicating a bag successfully") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val (srcBagRoot, _) = S3BagBuilder.createS3BagWith(
        bucket = srcBucket
      )

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
      )

      withLocalS3Bucket { dstBucket =>
        val result =
          withBagReplicatorWorker(
            bucket = dstBucket,
            ingests = ingests,
            outgoing = outgoing,
            stepName = "replicating"
          ) {
            _.processMessage(payload)
          }.success.value

        result shouldBe a[IngestStepSucceeded[_]]

        val receivedMessages =
          outgoing.getMessages[ReplicaResultPayload]

        receivedMessages.size shouldBe 1

        val receivedPayload = receivedMessages.head
        receivedPayload.context shouldBe payload.context
        receivedPayload.version shouldBe payload.version

        val dstBagRoot = receivedPayload.replicaResult.storageLocation.prefix

        verifyObjectsCopied(
          srcPrefix = srcBagRoot,
          dstPrefix = dstBagRoot
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

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagLocation
        )

        withLocalS3Bucket { dstBucket =>
          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload)
            }.success.value

          result shouldBe a[IngestStepSucceeded[_]]

          val destination = result.summary.dstPrefix
          destination.namespace shouldBe dstBucket.name
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
            bucket = srcBucket
          )

          val payload = createVersionedBagRootPayloadWith(
            bagRoot = srcBagLocation
          )

          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload)
            }.success.value

          result shouldBe a[IngestStepSucceeded[_]]

          val dstBagLocation = result.summary.dstPrefix
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

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagLocation
      )

      withLocalS3Bucket { dstBucket =>
        withBagReplicatorWorker(bucket = dstBucket) { worker =>
          val futures = (1 to 5).map { _ =>
            Future.successful(()).flatMap { _ =>
              Future.fromTry {
                worker.processMessage(payload)
              }
            }
          }

          whenReady(Future.sequence(futures)) { result =>
            println(result)
            result.count { _.isInstanceOf[IngestStepSucceeded[_]] } shouldBe 1
            result.count { _.isInstanceOf[IngestShouldRetry[_]] } shouldBe 4
          }
        }
      }
    }
  }

  it(
    "allows multiple workers for the same ingest to write to different locations"
  ) {
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
        payloadFileCount = 100
      )

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagLocation
      )

      val lockServiceDao = new MemoryLockDao[String, UUID] {}

      withLocalS3Bucket { dstBucket1 =>
        withLocalS3Bucket { dstBucket2 =>
          withBagReplicatorWorker(
            bucket = dstBucket1,
            lockServiceDao = lockServiceDao
          ) { worker1 =>
            withBagReplicatorWorker(
              bucket = dstBucket2,
              lockServiceDao = lockServiceDao
            ) { worker2 =>
              val futures = Seq(
                Future.successful(()).flatMap { _ =>
                  Future.fromTry {
                    worker1.processMessage(payload)
                  }
                },
                Future.successful(()).flatMap { _ =>
                  Future.fromTry {
                    worker2.processMessage(payload)
                  }
                }
              )

              whenReady(Future.sequence(futures)) { result =>
                println(result)
                result.count { _.isInstanceOf[IngestStepSucceeded[_]] } shouldBe 2
              }
            }
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

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagLocation
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

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagLocation
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

              implicit val badPrefixTransfer: S3PrefixTransfer =
                new S3PrefixTransfer()

              implicit val replicator = new S3Replicator() {
                override val prefixTransfer: S3PrefixTransfer =
                  badPrefixTransfer
              }

              implicit val s3StreamStore: S3StreamStore =
                new S3StreamStore()

              val bagReplicator =
                new BagReplicator(replicator)

              val service = new BagReplicatorWorker(
                config = createAlpakkaSQSWorkerConfig(queue),
                ingestUpdater = ingestUpdater,
                outgoingPublisher = outgoingPublisher,
                lockingService = lockingService,
                destinationConfig = replicatorDestinationConfig,
                bagReplicator = bagReplicator
              )

              val result = service.processMessage(payload).success.value

              result shouldBe a[IngestFailed[_]]

              val ingestFailed = result.asInstanceOf[IngestFailed[_]]
              ingestFailed.e.getMessage shouldBe "tagmanifest-sha256.txt in replica source and replica location do not match!"
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

          val payload = createVersionedBagRootPayloadWith(
            bagRoot = srcBagLocation
          )

          s3Client.deleteObject(
            srcBagLocation.namespace,
            srcBagLocation.asLocation("tagmanifest-sha256.txt").path
          )

          val result =
            withBagReplicatorWorker(queue, dstBucket) {
              _.processMessage(payload)
            }.success.value

          result shouldBe a[IngestFailed[_]]

          val ingestFailed = result.asInstanceOf[IngestFailed[_]]
          ingestFailed.e.getMessage should startWith(
            "Unable to load tagmanifest-sha256.txt in source and replica to compare:"
          )
        }
      }
    }
  }

  it("uses the provider configured in the destination config") {
    val provider = createProvider

    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val (srcBagRoot, _) = S3BagBuilder.createS3BagWith(
        bucket = srcBucket
      )

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
      )

      withLocalS3Bucket { dstBucket =>
        withBagReplicatorWorker(
          bucket = dstBucket,
          outgoing = outgoing,
          provider = provider
        ) {
          _.processMessage(payload)
        }.success.value

        outgoing
          .getMessages[ReplicaResultPayload]
          .head
          .replicaResult
          .storageLocation
          .provider shouldBe provider
      }
    }
  }

  describe("uses the request builder in the config") {
    it("primary replicas") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { srcBucket =>
        val (srcBagRoot, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            bucket = dstBucket,
            outgoing = outgoing,
            requestBuilder = PrimaryBagReplicationRequest.apply
          ) {
            _.processMessage(payload)
          }.success.value

          outgoing
            .getMessages[ReplicaResultPayload]
            .head
            .replicaResult
            .storageLocation shouldBe a[PrimaryStorageLocation]
        }
      }
    }

    it("secondary replicas") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { srcBucket =>
        val (srcBagRoot, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            bucket = dstBucket,
            outgoing = outgoing,
            requestBuilder = SecondaryBagReplicationRequest.apply
          ) {
            _.processMessage(payload)
          }.success.value

          outgoing
            .getMessages[ReplicaResultPayload]
            .head
            .replicaResult
            .storageLocation shouldBe a[SecondaryStorageLocation]
        }
      }
    }
  }
}
