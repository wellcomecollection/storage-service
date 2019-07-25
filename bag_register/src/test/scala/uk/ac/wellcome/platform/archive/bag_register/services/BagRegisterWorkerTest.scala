package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestCompleted

import scala.util.Success

class BagRegisterWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagInfoGenerators
    with BagRegisterFixtures
    with PayloadGenerators
    with TryValues {

  it("sends a successful IngestUpdate upon registration") {
    withBagRegisterWorker {
      case (service, storageManifestDao, ingests, _, _) =>
        val createdAfterDate = Instant.now()
        val space = createStorageSpace
        val version = randomInt(1, 15)

        withLocalS3Bucket { bucket =>
          withBag(bucket, space = space, version = version) {
            case (bagRootLocation, bagInfo) =>
              val payload = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = space
                ),
                bagRootLocation = bagRootLocation,
                version = version
              )

              val bagId = BagId(
                space = space,
                externalIdentifier = bagInfo.externalIdentifier
              )

              val result = service.processMessage(payload)
              result shouldBe a[Success[_]]
              result.success.value shouldBe a[IngestCompleted[_]]

              val storageManifest =
                storageManifestDao.getLatest(bagId).right.value

              storageManifest.space shouldBe bagId.space
              storageManifest.info shouldBe bagInfo
              storageManifest.manifest.files should have size 1

              storageManifest.locations shouldBe List(
                StorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  location = bagRootLocation.copy(
                    path = bagRootLocation.path.stripSuffix(s"/v$version")
                  )
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
        val space = createStorageSpace

        withLocalS3Bucket { bucket =>
          withBag(bucket, space = space, version = 1) {
            case (location1, bagInfo) =>
              withBag(bucket, space = space, version = 2) {
                case (location2, bagInfo) =>
                  val payload1 = createEnrichedBagInformationPayloadWith(
                    context = createPipelineContextWith(
                      storageSpace = space
                    ),
                    bagRootLocation = location1,
                    version = 1
                  )
                  val payload2 = createEnrichedBagInformationPayloadWith(
                    context = createPipelineContextWith(
                      storageSpace = space
                    ),
                    bagRootLocation = location2,
                    version = 2
                  )

                  val bagId = BagId(
                    space = space,
                    externalIdentifier = bagInfo.externalIdentifier
                  )

                  Seq(payload1, payload2).map { payload =>
                    val result = service.processMessage(payload)
                    result shouldBe a[Success[_]]
                    result.success.value shouldBe a[IngestCompleted[_]]
                  }

                  storageManifestDao
                    .get(bagId, version = 1)
                    .right
                    .value
                    .version shouldBe 1
                  storageManifestDao
                    .get(bagId, version = 2)
                    .right
                    .value
                    .version shouldBe 2

                  storageManifestDao
                    .getLatest(bagId)
                    .right
                    .value
                    .version shouldBe 2
              }
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
