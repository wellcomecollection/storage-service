package uk.ac.wellcome.platform.archive.bags.async.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bags.async.fixtures.{RegistrarFixtures, WorkerServiceFixture}
import uk.ac.wellcome.platform.archive.common.generators.BagIdGenerators
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagId, BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.storage.dynamo._

class BagsWorkerServiceTest extends FunSpec with Matchers with WorkerServiceFixture with RegistrarFixtures with ScalaFutures with ProgressUpdateAssertions with BagIdGenerators {
  it("sends a successful ProgressUpdate if it registers a Bag successfully") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withWorkerService(table, bucket, progressTopic) { service =>
            val archiveRequestId = randomUUID
            val createdAfterDate = Instant.now()
            val bagInfo = createBagInfo

            withBag(bucket, bagInfo = bagInfo) { archiveBagLocation =>
              val accessBagLocation = archiveBagLocation.copy(storagePrefix = Some("access"))

              val replicationResult = ReplicationResult(
                archiveRequestId = archiveRequestId,
                srcBagLocation = archiveBagLocation,
                dstBagLocation = accessBagLocation
              )

              val bagId = BagId(
                space = accessBagLocation.storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              val notification = createNotificationMessageWith(replicationResult)

              val future = service.processMessage(notification)

              whenReady(future) { _ =>
                val storageManifest = getStorageManifest(table, id = bagId)

                storageManifest.space shouldBe bagId.space
                storageManifest.info shouldBe bagInfo
                storageManifest.manifest.files should have size 1

                storageManifest.accessLocation shouldBe StorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  location = accessBagLocation.objectLocation
                )
                storageManifest.archiveLocations shouldBe List(
                  StorageLocation(
                    provider = InfrequentAccessStorageProvider,
                    location = archiveBagLocation.objectLocation
                  )
                )

                storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

                assertTopicReceivesProgressStatusUpdate(
                  requestId = archiveRequestId,
                  progressTopic = progressTopic,
                  status = Progress.Completed,
                  expectedBag = Some(bagId)) { events =>
                  events.size should be >= 1
                  events.head.description shouldBe "Bag registered successfully"
                }
              }
            }
          }
        }
      }
    }
  }

  it("sends a failed ProgressUpdate if it can't create a StorageManifest") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withWorkerService(table, bucket, progressTopic) { service =>
            val archiveRequestId = randomUUID

            val archiveBagLocation = BagLocation(
              storageNamespace = bucket.name,
              storagePrefix = Some(randomAlphanumeric()),
              storageSpace = createStorageSpace,
              bagPath = BagPath(randomAlphanumeric())
            )

            val accessBagLocation = archiveBagLocation.copy(storagePrefix = Some("access"))

            val replicationResult = ReplicationResult(
              archiveRequestId = archiveRequestId,
              srcBagLocation = archiveBagLocation,
              dstBagLocation = accessBagLocation
            )

            val notification = createNotificationMessageWith(replicationResult)

            val future = service.processMessage(notification)

            whenReady(future) { _ =>
              assertTopicReceivesProgressStatusUpdate(
                requestId = archiveRequestId,
                progressTopic = progressTopic,
                status = Progress.Failed,
                expectedBag = None) { events =>
                events.size should be >= 1
                events.head.description shouldBe "Failed to create storage manifest"
              }
            }
          }
        }
      }
    }
  }

  it("notifies the progress tracker if registering a bag fails") {
    withRegistrar {
      case (storageBucket, queuePair, progressTopic, vhs) =>
        val requestId = randomUUID
        val bagId = createBagId

        val srcBagLocation = BagLocation(
          storageNamespace = storageBucket.name,
          storagePrefix = Some("archive"),
          storageSpace = bagId.space,
          bagPath = randomBagPath
        )

        val dstBagLocation = srcBagLocation.copy(
          storagePrefix = Some("access")
        )

        sendNotificationToSQS(
          queuePair.queue,
          ReplicationResult(
            archiveRequestId = requestId,
            srcBagLocation = srcBagLocation,
            dstBagLocation = dstBagLocation
          )
        )

        eventually {
          val futureMaybeManifest = vhs.getRecord(bagId.toString)

          whenReady(futureMaybeManifest) { maybeStorageManifest =>
            maybeStorageManifest shouldNot be(defined)
          }

          assertTopicReceivesProgressStatusUpdate(
            requestId,
            progressTopic,
            Progress.Failed) { events =>
            events should have size 1
            events.head.description should startWith(
              "There was an exception while downloading object")
          }
        }
    }
  }

  it("discards messages if it fails writing to the VHS") {
    withRegistrarAndBrokenVHS {
      case (storageBucket, QueuePair(queue, dlq), progressTopic, _) =>
        withBagNotification(queue, storageBucket) { _ =>
          withBagNotification(queue, storageBucket) { _ =>
            eventually {
              listMessagesReceivedFromSNS(progressTopic) shouldBe empty

              assertQueueEmpty(queue)
              assertQueueHasSize(dlq, 2)
            }
          }
        }
    }
  }
}
