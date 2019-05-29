package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagInfoGenerators, PayloadGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}

import scala.util.Success

class BagRegisterWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagInfoGenerators
    with BagLocationFixtures
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends a successful IngestUpdate upon registration") {
    withBagRegisterWorker {
      case (service, dao, store, ingests, _, _) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withLocalS3Bucket { bucket =>
          withBag(bucket, bagInfo = bagInfo) {
            case (bagRootLocation, storageSpace) =>
              val payload = createBagInformationPayloadWith(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              val bagId = BagId(
                space = storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              service.processMessage(payload) shouldBe a[Success[_]]

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
          }
        }
    }
  }

  it("sends a failed IngestUpdate if storing fails") {
    withBagRegisterWorker {
      case (service, _, _, ingests, _, _) =>
        val payload = createBagInformationPayloadWith(
          bagRootLocation = createObjectLocation,
          storageSpace = createStorageSpace
        )

        service.processMessage(payload) shouldBe a[Success[_]]

        assertBagRegisterFailed(
          ingestId = payload.ingestId,
          ingests = ingests
        )
    }
  }
}
