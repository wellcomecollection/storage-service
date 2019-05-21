package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagInfoGenerators, IngestOperationGenerators, PayloadGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}

class BagRegisterFeatureTest
    extends FunSpec
    with SQS
    with Matchers
    with IngestOperationGenerators
    with BagInfoGenerators
    with BagLocationFixtures
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends an update if it registers a bag") {
    withBagRegisterWorker() {
      case (_, vhs, ingests, _, queuePair) =>
        withLocalS3Bucket { bucket =>
          val createdAfterDate = Instant.now()
          val bagInfo = createBagInfo

          withBag(storageBackend, namespace = bucket.name, bagInfo = bagInfo) {
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
                val storageManifest = vhs.getRecord(bagId).get.get

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

                assertBagRegisterSucceeded(ingests)(
                  ingestId = payload.ingestId,
                  bagId = bagId
                )

                assertQueueEmpty(queuePair.queue)
              }
          }
        }
    }
  }

  it("sends a failed update and discards the work on error") {
    withBagRegisterWorker(createBrokenStorageManifestVHS) {
      case (_, _, ingests, _, queuePair) =>
        withLocalS3Bucket { bucket =>
          val bagInfo = createBagInfo

          withBag(storageBackend, namespace = bucket.name, bagInfo = bagInfo) {
            case (bagRootLocation, storageSpace) =>
              val payload = createBagInformationPayloadWith(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              sendNotificationToSQS(queuePair.queue, payload)

              val bagId = BagId(
                space = storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              eventually {
                assertBagRegisterFailed(ingests)(
                  ingestId = payload.ingestId,
                  bagId = bagId
                )
              }

              assertQueueEmpty(queuePair.queue)
              assertQueueEmpty(queuePair.dlq)
          }
        }
    }
  }
}
