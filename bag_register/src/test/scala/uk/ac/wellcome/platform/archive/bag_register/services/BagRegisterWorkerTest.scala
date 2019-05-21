package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.storage.memory.MemoryStorageBackend

import scala.util.Success

class BagRegisterWorkerTest
    extends FunSpec
    with Matchers
    with BagInfoGenerators
    with BagLocationFixtures
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends a successful IngestUpdate upon registration") {
    val storageBackend = new MemoryStorageBackend()

    withBagRegisterWorker(storageBackend) {
      case (service, vhs, ingests, _, _) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withBag(storageBackend, bagInfo = bagInfo) {
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

            val storageManifest = vhs.getRecord(id = bagId).get.get

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
        }
    }
  }

  it("sends a failed IngestUpdate if storing fails") {
    val storageBackend = new MemoryStorageBackend()

    withBagRegisterWorker(storageBackend, vhs = createBrokenStorageManifestVHS) {
      case (service, _, ingests, _, _) =>
        val bagInfo = createBagInfo

        withBag(storageBackend, bagInfo = bagInfo) {
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

            assertBagRegisterFailed(ingests)(
              ingestId = payload.ingestId,
              bagId = bagId
            )
        }
    }
  }
}
