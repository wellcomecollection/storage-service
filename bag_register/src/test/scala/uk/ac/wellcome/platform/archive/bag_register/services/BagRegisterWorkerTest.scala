package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
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
      case (service, storageManifestDao, ingests, _, _) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withLocalS3Bucket { bucket =>
          withBag(bucket, bagInfo = bagInfo) {
            case (bagRootLocation, storageSpace) =>
              val payload = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = storageSpace
                ),
                bagRootLocation = bagRootLocation
              )

              val bagId = BagId(
                space = storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              val storageManifest = storageManifestDao.getLatest(bagId).right.value

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
                ingests = ingests
              )
          }
        }
    }
  }

  it("stores multiple versions of a bag") {
    withBagRegisterWorker {
      case (service, storageManifestDao, _, _, _) =>
        val bagInfo = createBagInfo

        withLocalS3Bucket { bucket =>
          withBag(bucket, bagInfo = bagInfo) {
            case (bagRootLocation, storageSpace) =>
              val payload1 = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = storageSpace
                ),
                bagRootLocation = bagRootLocation,
                version = 1
              )
              val payload2 = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = storageSpace
                ),
                bagRootLocation = bagRootLocation,
                version = 2
              )

              val bagId = BagId(
                space = storageSpace,
                externalIdentifier = bagInfo.externalIdentifier
              )

              service.processMessage(payload1) shouldBe a[Success[_]]
              service.processMessage(payload2) shouldBe a[Success[_]]

              storageManifestDao.get(bagId, version = 1).right.value.version shouldBe 1
              storageManifestDao.get(bagId, version = 2).right.value.version shouldBe 2

              storageManifestDao.getLatest(bagId).right.value.version shouldBe 2
          }
        }
    }
  }

  it("sends a failed IngestUpdate if storing fails") {
    withBagRegisterWorker {
      case (service, _, ingests, _, _) =>
        val payload = createEnrichedBagInformationPayload

        service.processMessage(payload) shouldBe a[Success[_]]

        assertBagRegisterFailed(
          ingestId = payload.ingestId,
          ingests = ingests
        )
    }
  }
}
