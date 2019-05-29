package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}

class BagRegisterFeatureTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends an update if it registers a bag") {
    withBagRegisterWorker() {
      case (_, dao, store, ingests, _, queuePair) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withLocalS3Bucket { bucket =>
          withBag(bucket, bagInfo = bagInfo) {
            case (bagRootLocation, storageSpace) =>
              val bagId = BagId(
                space = storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              val payload = createBagInformationPayloadWith(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              sendNotificationToSQS(queuePair.queue, payload)

              eventually {
                val storageManifest = getStorageManifest(dao, store, id = bagId)

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

                assertBagRegisterSucceeded(
                  ingestId = payload.ingestId,
                  ingests = ingests,
                  bagId = bagId
                )

                assertQueueEmpty(queuePair.queue)
              }
          }
        }
    }
  }

  it("sends a failed update and discards the work on error") {
    withBagRegisterWorker() {
      case (_, _, _, ingests, _, queuePair) =>
        val bagInfo = createBagInfo

        val payload = createBagInformationPayloadWith(
          bagRootLocation = createObjectLocation,
          storageSpace = createStorageSpace
        )

        sendNotificationToSQS(queuePair.queue, payload)

        val bagId = BagId(
          space = payload.storageSpace,
          externalIdentifier = bagInfo.externalIdentifier
        )

        eventually {
          assertBagRegisterFailed(
            ingestId = payload.ingestId,
            ingests = ingests,
            bagId = bagId
          )
        }

        assertQueueEmpty(queuePair.queue)
        assertQueueEmpty(queuePair.dlq)
    }
  }
}
