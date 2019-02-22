package uk.ac.wellcome.platform.archive.bags.async.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bags.async.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, BagInfoGenerators}
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagId, BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagsWorkerServiceTest
  extends FunSpec
  with Matchers
  with ScalaFutures
  with BagIdGenerators
  with BagInfoGenerators
  with BagLocationFixtures
  with ProgressUpdateAssertions
  with WorkerServiceFixture {

  it("sends a successful ProgressUpdate if it registers a Bag successfully") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withWorkerService(table, bucket, progressTopic) { service =>
            val createdAfterDate = Instant.now()
            val bagInfo = createBagInfo

            withBag(bucket, bagInfo = bagInfo) { archiveBagLocation =>
              val replicationResult = createReplicationResultWith(archiveBagLocation)
              val accessBagLocation = replicationResult.dstBagLocation

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
                  requestId = replicationResult.archiveRequestId,
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
            val archiveBagLocation = BagLocation(
              storageNamespace = bucket.name,
              storagePrefix = Some(randomAlphanumeric()),
              storageSpace = createStorageSpace,
              bagPath = BagPath(randomAlphanumeric())
            )

            val replicationResult = createReplicationResultWith(archiveBagLocation)

            val notification = createNotificationMessageWith(replicationResult)

            val future = service.processMessage(notification)

            whenReady(future) { _ =>
              assertTopicReceivesProgressStatusUpdate(
                requestId = replicationResult.archiveRequestId,
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

  it("sends a failed ProgressUpdate if updating the VHS fails") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withWorkerService(table, Bucket("does-not-exist"), progressTopic) { service =>
            val bagInfo = createBagInfo

            withBag(bucket, bagInfo = bagInfo) { archiveBagLocation =>
              val replicationResult = createReplicationResultWith(archiveBagLocation)
              val accessBagLocation = replicationResult.dstBagLocation

              val bagId = BagId(
                space = accessBagLocation.storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              val notification = createNotificationMessageWith(replicationResult)

              val future = service.processMessage(notification)

              whenReady(future) { _ =>
                assertTopicReceivesProgressStatusUpdate(
                  requestId = replicationResult.archiveRequestId,
                  progressTopic = progressTopic,
                  status = Progress.Failed,
                  expectedBag = Some(bagId)) { events =>
                  events.size should be >= 1
                  events.head.description shouldBe "Failed to register bag"
                }
              }
            }
          }
        }
      }
    }
  }
}
