package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths
import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.fixtures.{
  S3BagBuilder,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestShouldRetry,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.locking.{LockDao, LockFailure}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with TryValues
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
        val serviceResult =
          withBagReplicatorWorker(
            bucket = dstBucket,
            ingests = ingests,
            outgoing = outgoing,
            stepName = "replicating"
          ) {
            _.processMessage(payload)
          }

        serviceResult.success.value shouldBe a[IngestStepSucceeded[_]]

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
          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload).success.value
            }

          result shouldBe a[IngestStepSucceeded[_]]

          val destination = result.summary.dstPrefix
          destination.namespace shouldBe dstBucket.name
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val rootPath = randomAlphanumericWithLength()

          val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
            bucket = srcBucket
          )

          val payload = createEnrichedBagInformationPayloadWith(
            bagRootLocation = srcBagLocation
          )

          val result =
            withBagReplicatorWorker(
              bucket = dstBucket,
              rootPath = Some(rootPath)
            ) {
              _.processMessage(payload).success.value
            }

          result shouldBe a[IngestStepSucceeded[_]]

          val dstBagLocation = result.summary.dstPrefix
          val expectedPath =
            Paths
              .get(
                rootPath,
                payload.storageSpace.underlying,
                payload.externalIdentifier.toString,
                payload.version.toString
              )
              .toString
          dstBagLocation.path shouldBe expectedPath
        }
      }
    }

    it("key ends with the external identifier and version of the bag") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
            bucket = srcBucket
          )

          val payload = createEnrichedBagInformationPayloadWith(
            bagRootLocation = srcBagLocation,
            version = BagVersion(3)
          )

          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload).success.value
            }

          result shouldBe a[IngestStepSucceeded[_]]

          val dstBagLocation = result.summary.dstPrefix
          dstBagLocation.path should endWith(
            s"/${payload.externalIdentifier.toString}/v3"
          )
        }
      }
    }

    it("prefixes the key with the storage space if no root path is set") {
      withLocalS3Bucket { srcBucket =>
        val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagLocation
        )

        withLocalS3Bucket { dstBucket =>
          val result =
            withBagReplicatorWorker(bucket = dstBucket) {
              _.processMessage(payload).success.value
            }

          result shouldBe a[IngestStepSucceeded[_]]

          val dstBagLocation = result.summary.dstPrefix
          dstBagLocation.path should startWith(payload.storageSpace.underlying)
        }
      }
    }

    it("prefixes the key with the root path if set") {
      withLocalS3Bucket { srcBucket =>
        val (srcBagLocation, _) = S3BagBuilder.createS3BagWith(
          bucket = srcBucket
        )

        val payload = createEnrichedBagInformationPayloadWith(
          bagRootLocation = srcBagLocation
        )

        withLocalS3Bucket { dstBucket =>
          val result =
            withBagReplicatorWorker(
              bucket = dstBucket,
              rootPath = Some("rootprefix")
            ) {
              _.processMessage(payload).success.value
            }

          val dstBagLocation = result.summary.dstPrefix
          dstBagLocation.path should startWith("rootprefix/")
        }
      }
    }
  }

  it("only allows one worker to process a destination") {
    withLocalS3Bucket { srcBucket =>
      // We have to create a large bag to slow down the replicators, or the
      // first process finishes and releases the lock before the later
      // processes have started.
      val bagBuilder = new S3BagBuilderBase {
        override def getFetchEntryCount(payloadFileCount: Int): Int = 0
      }

      val (srcBagLocation, _) = bagBuilder.createS3BagWith(
        bucket = srcBucket,
        payloadFileCount = 500
      )

      val payload = createEnrichedBagInformationPayloadWith(
        bagRootLocation = srcBagLocation
      )

      withLocalS3Bucket { dstBucket =>
        withBagReplicatorWorker(bucket = dstBucket) { worker =>
          val futures: Future[Seq[IngestStepResult[ReplicationSummary]]] =
            Future.sequence(
              (1 to 5).map { i =>
                Future.successful(i).flatMap {
                  _ =>
                    // Introduce a tiny bit of fudge to cope with the fact that the memory
                    // locking service isn't thread-safe.
                    Thread.sleep(i * 150)

                    // We can't just wrap the Try directly, because Future.fromTry
                    // waits for the Try to finish -- flat-mapping a Future.successful()
                    // allows us to run multiple Try processes at once.
                    Future.fromTry {
                      worker.processMessage(payload)
                    }
                }
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
}
