package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bag_register.fixtures.WorkerFixture
import uk.ac.wellcome.platform.archive.bag_register.services.BagRegisterWorker
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, BagInfoGenerators}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagRegisterFeatureTest
  extends FunSpec
    with Matchers
    with BagIdGenerators
    with BagInfoGenerators
    with BagLocationFixtures
    with ProgressUpdateAssertions
    with WorkerFixture {

  it("sends an update if it registers a bag") {
    withWorkerService() {
      case (
        _: BagRegisterWorker,
        table: Table,
        bucket: Bucket,
        progressTopic: Topic,
        _: Topic,
        queuePair: QueuePair
        ) => {

        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo
        val storagePrefix = "storagePrefix"

        withBag(bucket, bagInfo, storagePrefix) { location =>

          val bagId = BagId(
            space = location.storageSpace,
            externalIdentifier = bagInfo.externalIdentifier
          )

          val bagRequest = createBagRequestWith(location)

          sendNotificationToSQS(queuePair.queue, bagRequest)

          eventually {
            val storageManifest = getStorageManifest(table, id = bagId)

            storageManifest.space shouldBe bagId.space
            storageManifest.info shouldBe bagInfo
            storageManifest.manifest.files should have size 1

            storageManifest.accessLocation shouldBe StorageLocation(
              provider = InfrequentAccessStorageProvider,
              location = location.objectLocation
            )

            storageManifest.archiveLocations shouldBe List.empty

            storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

            assertTopicReceivesProgressStatusUpdate(
              requestId = bagRequest.requestId,
              progressTopic = progressTopic,
              status = Progress.Completed,
              expectedBag = Some(bagId)) { events =>
              events.size should be >= 1
              events.head.description shouldBe "Register succeeded (completed)"
            }

            assertQueueEmpty(queuePair.queue)
          }
        }
      }
    }
  }

  it("sends a failed update and discards the work on error") {
    withWorkerService(userBucket = Some(Bucket("does_not_exist"))) {
      case (
        _: BagRegisterWorker,
        _: Table,
        bucket: Bucket,
        progressTopic: Topic,
        _: Topic,
        queuePair: QueuePair
        ) => {

        val bagInfo = createBagInfo

        withBag(bucket, bagInfo = bagInfo) {
          accessBagLocation =>
            val replicationResult =
              createBagRequestWith(accessBagLocation)

            sendNotificationToSQS(queuePair.queue, replicationResult)

            eventually {
              assertTopicReceivesProgressStatusUpdate(
                requestId = replicationResult.requestId,
                progressTopic = progressTopic,
                status = Progress.Failed,
                expectedBag =
                  Some(
                    BagId(
                      space = accessBagLocation.storageSpace,
                      externalIdentifier = bagInfo.externalIdentifier
                    )
                  )
              ) { events =>
                events.size should be >= 1
                events.head.description shouldBe "Register failed"
              }
            }

            assertQueueEmpty(queuePair.queue)
            assertQueueEmpty(queuePair.dlq)
        }
      }
    }
  }
}
