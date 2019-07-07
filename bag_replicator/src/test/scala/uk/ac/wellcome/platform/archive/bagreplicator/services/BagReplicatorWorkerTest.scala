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
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestShouldRetry, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.storage.locking.{LockDao, LockFailure}
import uk.ac.wellcome.storage.locking.memory.MemoryLockDao

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with S3BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with TryValues
    with ScalaFutures {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        withBagReplicatorWorker(
          bucket = archiveBucket,
          ingests = ingests,
          outgoing = outgoing,
          stepName = "replicating") { service =>
          withBagObjects(ingestsBucket) { srcBagRootLocation =>
            val payload = createEnrichedBagInformationPayloadWith(
              bagRootLocation = srcBagRootLocation
            )

            val serviceResult = service.processMessage(payload)
            serviceResult.success.value shouldBe a[IngestStepSucceeded[_]]

            val receivedMessages =
              outgoing.getMessages[EnrichedBagInformationPayload]

            receivedMessages.size shouldBe 1

            val result = receivedMessages.head
            result.ingestId shouldBe payload.ingestId

            val dstBagRootLocation = result.bagRootLocation

            verifyObjectsCopied(
              src = srcBagRootLocation,
              dst = dstBagRootLocation
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
    }
  }

  describe("copies to the correct destination") {
    it("copies the bag to the configured bucket") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(bucket = archiveBucket) { worker =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              val result = worker.processMessage(payload).success.value
              result shouldBe a[IngestStepSucceeded[_]]

              val destination = result.summary.destination
              destination.namespace shouldBe archiveBucket.name
            }
          }
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          val rootPath = randomAlphanumericWithLength()
          withBagReplicatorWorker(
            bucket = archiveBucket,
            rootPath = Some(rootPath)) { worker =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              val result = worker.processMessage(payload).success.value
              result shouldBe a[IngestStepSucceeded[_]]

              val destination = result.summary.destination
              val expectedPath =
                Paths
                  .get(
                    rootPath,
                    payload.storageSpace.underlying,
                    payload.externalIdentifier.toString,
                    s"v${payload.version}"
                  )
                  .toString
              destination.path shouldBe expectedPath
            }
          }
        }
      }
    }

    it("key ends with the external identifier and version of the bag") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(bucket = archiveBucket) { worker =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation,
                version = 3
              )

              val result = worker.processMessage(payload).success.value
              result shouldBe a[IngestStepSucceeded[_]]

              val destination = result.summary.destination
              destination.path should endWith(
                s"/${payload.externalIdentifier.toString}/v3")
            }
          }
        }
      }
    }

    it("prefixes the key with the storage space if no root path is set") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(bucket = archiveBucket) { worker =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              val result = worker.processMessage(payload).success.value
              result shouldBe a[IngestStepSucceeded[_]]

              val destination = result.summary.destination
              destination.path should startWith(
                payload.storageSpace.underlying)
            }
          }
        }
      }
    }

    it("prefixes the key with the root path if set") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(
            bucket = archiveBucket,
            rootPath = Some("rootprefix")) { worker =>
            withBagObjects(ingestsBucket) { bagRootLocation =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              val result = worker.processMessage(payload).success.value
              result shouldBe a[IngestStepSucceeded[_]]

              val destination = result.summary.destination
              destination.path should startWith("rootprefix/")
            }
          }
        }
      }
    }
  }

  it("locks around the destination") {
    val lockServiceDao = new MemoryLockDao[String, UUID] {}

    withLocalS3Bucket { bucket =>
      withBagReplicatorWorker(bucket = bucket, lockServiceDao = lockServiceDao) {
        service =>
          withBagObjects(bucket) { bagRootLocation =>
            val payload = createEnrichedBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            val result = service.processMessage(payload).success.value
            result shouldBe a[IngestStepSucceeded[_]]

            val destination = result.summary.destination

            // TODO: Restore these history tests
            println(destination)
          // lockServiceDao.history should have size 1
          // lockServiceDao.history.head.id shouldBe destination.toString
          }
      }
    }
  }

  it("only allows one worker to process a destination") {
    val lockServiceDao = new MemoryLockDao[String, UUID] {}

    withLocalS3Bucket { bucket =>
      // We have to create a large bag to slow down the replicators, or the
      // first process finishes and releases the lock before the later
      // processes have started.
      withBagObjects(bucket) { bagRootLocation =>
        withBagReplicatorWorker(
          bucket = bucket,
          lockServiceDao = lockServiceDao) { worker =>
          val payload = createEnrichedBagInformationPayloadWith(
            bagRootLocation = bagRootLocation
          )

          val futures: Future[Seq[IngestStepResult[ReplicationSummary]]] =
            Future.sequence(
              (1 to 5).map { i =>
                Future.successful(i).flatMap { _ =>
                  // Introduce a tiny bit of fudge to cope with the fact that the memory
                  // locking service isn't thread-safe.
                  Thread.sleep(i * 150)

                  Future.fromTry {
                    worker.processMessage(payload)
                  }
                }
              }
            )

          whenReady(futures) { result =>
            result.count { _.isInstanceOf[IngestStepSucceeded[_]] } shouldBe 1
            result.count { _.isInstanceOf[IngestShouldRetry[_]] } shouldBe 4

          // TODO: Restore this test
          // lockServiceDao.history should have size 1
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

    withLocalS3Bucket { bucket =>
      withBagObjects(bucket) { bagRootLocation =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withLocalSqsQueue { queue =>
          withBagReplicatorWorker(
            queue = queue,
            bucket = bucket,
            ingests = ingests,
            outgoing = outgoing,
            lockServiceDao = neverAllowLockDao) { _ =>
            val payload = createEnrichedBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

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
                      "ApproximateNumberOfMessages").asJava
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
