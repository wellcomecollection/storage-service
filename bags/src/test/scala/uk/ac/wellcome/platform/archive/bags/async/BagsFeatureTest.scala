package uk.ac.wellcome.platform.archive.bags.async

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bags.async.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  BagInfoGenerators
}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagsFeatureTest
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
          withLocalSqsQueue { queue =>
            withWorkerService(table, bucket, progressTopic, queue) { service =>
              val createdAfterDate = Instant.now()
              val bagInfo = createBagInfo

              withBag(bucket, bagInfo = bagInfo, storagePrefix = "access") {
                accessBagLocation =>
                  val replicationResult =
                    createReplicationResultWith(accessBagLocation)
                  val archiveBagLocation = replicationResult.srcBagLocation

                  val bagId = BagId(
                    space = archiveBagLocation.storageSpace,
                    externalIdentifier = bagInfo.externalIdentifier
                  )

                  sendNotificationToSQS(queue, replicationResult)

                  eventually {
                    val storageManifest = getStorageManifest(table, id = bagId)

                    storageManifest.space shouldBe bagId.space
                    storageManifest.info shouldBe bagInfo
                    storageManifest.manifest.files should have size 1

                    storageManifest.accessLocation shouldBe StorageLocation(
                      provider = InfrequentAccessStorageProvider,
                      location = accessBagLocation.objectLocation
                    )
                    storageManifest.archiveLocations shouldBe List.empty

                    storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

                    assertTopicReceivesProgressStatusUpdate(
                      requestId = replicationResult.archiveRequestId,
                      progressTopic = progressTopic,
                      status = Progress.Completed,
                      expectedBag = Some(bagId)) { events =>
                      events.size should be >= 1
                      events.head.description shouldBe "Bag registered successfully"
                    }

                    assertQueueEmpty(queue)
                  }
              }
            }
          }
        }
      }
    }
  }

  it(
    "sends a failed ProgressUpdate and discards SQS messages if something goes wrong") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withLocalSqsQueueAndDlq {
            case QueuePair(queue, dlq) =>
              withWorkerService(
                table,
                Bucket("does-not-exist"),
                progressTopic,
                queue) { service =>
                val bagInfo = createBagInfo

                withBag(bucket, bagInfo = bagInfo, storagePrefix = "access") {
                  accessBagLocation =>
                    val replicationResult =
                      createReplicationResultWith(accessBagLocation)

                    val bagId = BagId(
                      space = accessBagLocation.storageSpace,
                      externalIdentifier = bagInfo.externalIdentifier
                    )

                    val notification =
                      createNotificationMessageWith(replicationResult)

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

                    assertQueueEmpty(queue)
                    assertQueueEmpty(dlq)
                }
              }
          }
        }
      }
    }
  }
}
