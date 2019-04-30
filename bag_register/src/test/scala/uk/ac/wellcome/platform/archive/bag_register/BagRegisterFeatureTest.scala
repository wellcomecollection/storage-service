package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  IngestOperationGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  Ingest,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagRegisterFeatureTest
    extends FunSpec
    with SQS
    with Matchers
    with IngestOperationGenerators
    with BagInfoGenerators
    with BagLocationFixtures
    with IngestUpdateAssertions
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends an update if it registers a bag") {
    withBagRegisterWorker {
      case (_, table, bucket, ingestTopic, _, queuePair) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withBag(bucket, bagInfo = bagInfo) {
          case (bagRootLocation, storageSpace) =>
            val bagId = BagId(
              space = storageSpace,
              externalIdentifier = bagInfo.externalIdentifier
            )

            val payload = createObjectLocationPayloadWith(
              objectLocation = bagRootLocation,
              storageSpace = storageSpace
            )

            sendNotificationToSQS(queuePair.queue, payload)

            eventually {
              val storageManifest = getStorageManifest(table, id = bagId)

              storageManifest.space shouldBe bagId.space
              storageManifest.info shouldBe bagInfo
              storageManifest.manifest.files should have size 1

              storageManifest.locations shouldBe List(
                StorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  location = bagRootLocation
                )
              )

              storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

              assertTopicReceivesIngestStatus(
                ingestId = payload.ingestId,
                ingestTopic = ingestTopic,
                status = Ingest.Completed,
                expectedBag = Some(bagId)) { events =>
                events.size should be >= 1
                events.head.description shouldBe "Register succeeded (completed)"
              }

              assertQueueEmpty(queuePair.queue)
            }
        }
    }
  }

  it("sends a failed update and discards the work on error") {
    withBagRegisterWorkerAndBucket(Bucket("does_not_exist")) {
      case (_, _, bucket, ingestTopic, _, queuePair) =>
        val bagInfo = createBagInfo

        withBag(bucket, bagInfo = bagInfo) {
          case (bagRootLocation, storageSpace) =>
            val payload = createObjectLocationPayloadWith(
              objectLocation = bagRootLocation,
              storageSpace = storageSpace
            )

            sendNotificationToSQS(queuePair.queue, payload)

            eventually {
              assertTopicReceivesIngestStatus(
                ingestId = payload.ingestId,
                ingestTopic = ingestTopic,
                status = Ingest.Failed,
                expectedBag = Some(
                  BagId(
                    space = storageSpace,
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
