package weco.storage_service.bag_replicator.services

import java.nio.file.Paths
import java.util.UUID
import org.scalatest.TryValues
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.QueuePair
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.bag_replicator.fixtures.BagReplicatorFixtures
import weco.storage_service.bag_replicator.models.{
  PrimaryReplica,
  SecondaryReplica
}
import weco.storage_service.ReplicaCompletePayload
import weco.storage_service.fixtures.s3.S3BagBuilder
import weco.storage_service.generators.PayloadGenerators
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.storage.models._
import weco.storage.locking.memory.MemoryLockDao
import weco.storage.locking.{LockDao, LockFailure}
import weco.storage.s3.S3ObjectLocationPrefix

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagReplicatorWorkerTest
    extends AnyFunSpec
    with Matchers
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with ScalaFutures
    with IntegrationPatience
    with TryValues
    with S3BagBuilder {

  it("replicating a bag successfully") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val (srcBagRoot, _) = storeBagWith()(
        namespace = srcBucket,
        primaryBucket = srcBucket
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
          outgoing.getMessages[ReplicaCompletePayload]

        receivedMessages.size shouldBe 1

        val receivedPayload = receivedMessages.head
        receivedPayload.context shouldBe payload.context
        receivedPayload.version shouldBe payload.version

        val dstBagRoot = receivedPayload.dstLocation.prefix
          .asInstanceOf[S3ObjectLocationPrefix]

        verifyObjectsCopied(
          srcPrefix = srcBagRoot,
          dstPrefix = dstBagRoot
        )

        assertTopicReceivesIngestEvents(
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
        val (srcBagRoot, _) = storeBagWith()(
          namespace = srcBucket,
          primaryBucket = srcBucket
        )

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalS3Bucket { dstBucket =>
          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload)
            }.success.value

          result shouldBe a[IngestStepSucceeded[_]]

          val destination = result.summary.request.dstPrefix
          destination.namespace shouldBe dstBucket.name
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val (srcBagRoot, _) = storeBagWith()(
            namespace = srcBucket,
            primaryBucket = srcBucket
          )

          val payload = createVersionedBagRootPayloadWith(
            bagRoot = srcBagRoot
          )

          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload)
            }.success.value

          result shouldBe a[IngestStepSucceeded[_]]

          val dstBagLocation = result.summary.request.dstPrefix
          val expectedPath =
            Paths
              .get(
                payload.storageSpace.underlying,
                payload.externalIdentifier.toString,
                payload.version.toString
              )
              .toString
          dstBagLocation.keyPrefix shouldBe expectedPath
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
      val bagBuilder = new S3BagBuilder {
        override def getFetchEntryCount(payloadFileCount: Int): Int = 0
      }

      val (srcBagRoot, _) = bagBuilder.storeBagWith(
        payloadFileCount = 50
      )(namespace = srcBucket, primaryBucket = srcBucket)

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
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
      val bagBuilder = new S3BagBuilder {
        override def getFetchEntryCount(payloadFileCount: Int): Int = 0
      }

      val (srcBagRoot, _) = bagBuilder.storeBagWith(
        payloadFileCount = 100
      )(namespace = srcBucket, primaryBucket = srcBucket)

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
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
      val (srcBagRoot, _) = storeBagWith()(
        namespace = srcBucket,
        primaryBucket = srcBucket
      )

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
      )

      withLocalSqsQueuePair(visibilityTimeout = 1.second) { case QueuePair(queue, dlq) =>
        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            queue = queue,
            bucket = dstBucket,
            lockServiceDao = neverAllowLockDao,
            visibilityTimeout = 1.second
          ) { _ =>
            sendNotificationToSQS(queue, payload)

            eventually {
              assertQueueEmpty(queue)
              assertQueueHasSize(dlq, size = 1)
            }
          }
        }
      }
    }
  }

  describe("chooses the primary/secondary replica appropriately") {
    it("primary replicas") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { srcBucket =>
        val (srcBagRoot, _) = storeBagWith()(
          namespace = srcBucket,
          primaryBucket = srcBucket
        )

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            bucket = dstBucket,
            outgoing = outgoing,
            replicaType = PrimaryReplica
          ) {
            _.processMessage(payload)
          }.success.value

          outgoing
            .getMessages[ReplicaCompletePayload]
            .head
            .dstLocation shouldBe a[PrimaryS3ReplicaLocation]
        }
      }
    }

    it("secondary replicas") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { srcBucket =>
        val (srcBagRoot, _) = storeBagWith()(
          namespace = srcBucket,
          primaryBucket = srcBucket
        )

        val payload = createVersionedBagRootPayloadWith(
          bagRoot = srcBagRoot
        )

        withLocalS3Bucket { dstBucket =>
          withBagReplicatorWorker(
            bucket = dstBucket,
            outgoing = outgoing,
            replicaType = SecondaryReplica
          ) {
            _.processMessage(payload)
          }.success.value

          outgoing
            .getMessages[ReplicaCompletePayload]
            .head
            .dstLocation shouldBe a[SecondaryS3ReplicaLocation]
        }
      }
    }
  }
}
