package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths
import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.memory.MemoryLockDao
import uk.ac.wellcome.storage.{LockDao, LockFailure}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with TryValues
    with ScalaFutures {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val ingests = createMessageSender
        val outgoing = createMessageSender

        withBagReplicatorWorker(bucket = archiveBucket, ingests = ingests, outgoing = outgoing) {
          service =>
            withBag(ingestsBucket) {
              case (srcBagRootLocation, storageSpace) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = srcBagRootLocation,
                  storageSpace = storageSpace
                )

                val serviceResult = service.processMessage(payload)
                serviceResult.success.value shouldBe a[Successful[_]]

                val receivedMessages =
                  outgoing.getMessages[BagInformationPayload]
                receivedMessages.size shouldBe 1

                val result = receivedMessages.head
                result.ingestId shouldBe payload.ingestId

                val dstBagRootLocation = result.bagRootLocation

                verifyBagCopied(
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
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val result = worker.processMessage(payload).success.value
                result shouldBe a[Successful[_]]

                val destination = result.summary.get.destination
                destination.namespace shouldBe archiveBucket.name
            }
          }
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          val rootPath = randomAlphanumeric()
          withBagReplicatorWorker(
            bucket = archiveBucket,
            rootPath = Some(rootPath)) { worker =>
            val bagInfo = createBagInfo
            withBag(ingestsBucket, bagInfo = bagInfo) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val result = worker.processMessage(payload).success.value
                result shouldBe a[Successful[_]]

                val destination = result.summary.get.destination
                val expectedKey =
                  Paths
                    .get(
                      rootPath,
                      payload.storageSpace.underlying,
                      payload.externalIdentifier.toString,
                      s"v${payload.version}"
                    )
                    .toString
                destination.key shouldBe expectedKey
            }
          }
        }
      }
    }

    it("key ends with the external identifier and version of the bag") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(bucket = archiveBucket) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation,
                  version = 3
                )

                val result = worker.processMessage(payload).success.value
                result shouldBe a[Successful[_]]

                val destination = result.summary.get.destination
                destination.key should endWith(
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
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val result = worker.processMessage(payload).success.value
                result shouldBe a[Successful[_]]

                val destination = result.summary.get.destination
                destination.key should startWith(
                  payload.storageSpace.underlying)
            }
          }
        }
      }
    }

    it("prefixes the key with the root path if set") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(bucket = archiveBucket, rootPath = Some("rootprefix")) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val result = worker.processMessage(payload).success.value
                result shouldBe a[Successful[_]]

                val destination = result.summary.get.destination
                destination.key should startWith("rootprefix/")
            }
          }
        }
      }
    }
  }

  it("locks around the destination") {
    val lockServiceDao = new MemoryLockDao[String, UUID] {}

    withBagReplicatorWorker(lockServiceDao = lockServiceDao) { service =>
      val payload = createBagInformationPayload
      val result = service.processMessage(payload).success.value
      result shouldBe a[Successful[_]]

      val destination = result.summary.get.destination

      lockServiceDao.history should have size 1
      lockServiceDao.history.head.id shouldBe destination.toString
    }
  }

  it("only allows one worker to process a destination") {
    val lockServiceDao = new MemoryLockDao[String, UUID] {}

    withLocalS3Bucket { bucket =>
      // We have to create a large bag to slow down the replicators, or the
      // first process finishes and releases the lock before the later
      // processes have started.
      withBag(bucket, dataFileCount = 250) {
        case (bagRootLocation, _) =>
          withBagReplicatorWorker(lockServiceDao = lockServiceDao) { worker =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            val futures: Future[Seq[Result[ReplicationSummary]]] =
              Future.sequence(
                (1 to 5).map { i =>
                  Future.successful(i).flatMap { _ =>
                    Future.fromTry { worker.processMessage(payload) }
                  }
                }
              )

            whenReady(futures) { result =>
              result.count { _.isInstanceOf[Successful[_]] } shouldBe 1
              result.count { _.isInstanceOf[NonDeterministicFailure[_]] } shouldBe 4

              lockServiceDao.history should have size 1
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
      withBag(bucket, dataFileCount = 20) {
        case (bagRootLocation, _) =>
          val ingests = createMessageSender
          val outgoing = createMessageSender
          withLocalSqsQueue { queue =>
            withBagReplicatorWorker(
              queue = queue,
              bucket = bucket,
              ingests = ingests,
              outgoing = outgoing,
              lockServiceDao = neverAllowLockDao) { _ =>
              val payload = createBagInformationPayloadWith(
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
