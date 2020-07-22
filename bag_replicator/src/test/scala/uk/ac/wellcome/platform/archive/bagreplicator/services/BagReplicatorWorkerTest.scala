package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths
import java.util.UUID

import org.scalatest.TryValues
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.models.{
  PrimaryReplica,
  SecondaryReplica
}
import uk.ac.wellcome.platform.archive.common.ReplicaCompletePayload
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.locking.memory.MemoryLockDao
import uk.ac.wellcome.storage.locking.{LockDao, LockFailure}

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
      val (srcBagRoot, _) = createS3BagWith(
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
        val (srcBagRoot, _) = createS3BagWith(
          bucket = srcBucket
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
          val (srcBagRoot, _) = createS3BagWith(
            bucket = srcBucket
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

      val (srcBagRoot, _) = bagBuilder.createS3BagWith(
        bucket = srcBucket,
        payloadFileCount = 50
      )

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

      val (srcBagRoot, _) = bagBuilder.createS3BagWith(
        bucket = srcBucket,
        payloadFileCount = 100
      )

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
      val (srcBagRoot, _) = createS3BagWith(
        bucket = srcBucket
      )

      val payload = createVersionedBagRootPayloadWith(
        bagRoot = srcBagRoot
      )

      withLocalSqsQueue() { queue =>
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
              val messagesNotVisible = getQueueAttribute(
                queue,
                attributeName =
                  QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE
              )

              assert(
                messagesNotVisible == "1",
                s"Expected ${queue.url} to have 1 visible message, actually found $messagesNotVisible"
              )

              val messagesVisible = getQueueAttribute(
                queue,
                attributeName =
                  QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES
              )

              assert(
                messagesVisible == "0",
                s"Expected ${queue.url} to have no visible messages, actually found $messagesVisible"
              )
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
        val (srcBagRoot, _) = createS3BagWith(
          bucket = srcBucket
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
        val (srcBagRoot, _) = createS3BagWith(
          bucket = srcBucket
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
