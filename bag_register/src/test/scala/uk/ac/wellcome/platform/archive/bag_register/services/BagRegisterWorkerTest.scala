package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.{BagInfoGenerators, PayloadGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestCompleted, KnownReplicas, PrimaryStorageLocation}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

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
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    withBagRegisterWorker {
      case (service, storageManifestDao, ingests, _, _) =>
        val createdAfterDate = Instant.now()
        val space = createStorageSpace
        val version = createBagVersion
        val dataFileCount = randomInt(1, 15)
        val externalIdentifier = createExternalIdentifier

        withNamespace { implicit namespace =>
          withRegisterBag(
            externalIdentifier,
            space = space,
            dataFileCount = dataFileCount,
            version = version
          ) {
            case (bagRoot, bagInfo) =>

              val knownReplicas = KnownReplicas(
                location = PrimaryStorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  prefix = bagRoot.asPrefix
                ),
                replicas = List.empty
              )

              val payload = createKnownReplicasPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = space
                ),
                version = version,
                knownReplicas = knownReplicas
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
              storageManifest.manifest.files should have size dataFileCount

              storageManifest.location shouldBe PrimaryStorageLocation(
                provider = InfrequentAccessStorageProvider,
                prefix = bagRoot
                  .copy(
                    path = bagRoot.path.stripSuffix(s"/$version")
                  )
                  .asPrefix
              )

              storageManifest.replicaLocations shouldBe empty

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
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val version1 = createBagVersion
    val version2 = version1.increment

    withBagRegisterWorker {
      case (service, storageManifestDao, _, _, _) =>
        val space = createStorageSpace
        val dataFileCount = randomInt(1, 15)
        val externalIdentifier = createExternalIdentifier

        withNamespace { implicit namespace =>
          withRegisterBag(
            externalIdentifier,
            space = space,
            version = version1,
            dataFileCount
          ) {
            case (location1, bagInfo1) =>
              withRegisterBag(
                externalIdentifier,
                space = space,
                version = version2,
                dataFileCount
              ) {
                case (location2, _) =>

                  val knownReplicas1 = KnownReplicas(
                    location = PrimaryStorageLocation(
                      provider = InfrequentAccessStorageProvider,
                      prefix = location1.asPrefix
                    ),
                    replicas = List.empty
                  )

                  val payload1 = createKnownReplicasPayloadWith(
                    context = createPipelineContextWith(
                      storageSpace = space
                    ),
                    version = version1,
                    knownReplicas = knownReplicas1
                  )

                  val knownReplicas2 = KnownReplicas(
                    location = PrimaryStorageLocation(
                      provider = InfrequentAccessStorageProvider,
                      prefix = location2.asPrefix
                    ),
                    replicas = List.empty
                  )

                  val payload2 = createKnownReplicasPayloadWith(
                    context = createPipelineContextWith(
                      storageSpace = space
                    ),
                    version = version2,
                    knownReplicas = knownReplicas2
                  )

                  val bagId = BagId(
                    space = space,
                    externalIdentifier = bagInfo1.externalIdentifier
                  )

                  Seq(payload1, payload2).map { payload =>
                    val result = service.processMessage(payload)
                    result shouldBe a[Success[_]]
                    result.success.value shouldBe a[IngestCompleted[_]]
                  }

                  storageManifestDao
                    .get(bagId, version = version1)
                    .right
                    .value
                    .version shouldBe version1
                  storageManifestDao
                    .get(bagId, version = version2)
                    .right
                    .value
                    .version shouldBe version2

                  storageManifestDao
                    .getLatest(bagId)
                    .right
                    .value
                    .version shouldBe version2
              }
          }
        }
    }
  }

  it("sends a failed IngestUpdate if storing fails") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    withBagRegisterWorker {
      case (service, _, ingests, _, _) =>

        val payload = createKnownReplicasPayload

        service.processMessage(payload) shouldBe a[Success[_]]

        assertBagRegisterFailed(
          ingestId = payload.ingestId,
          ingests = ingests
        )
    }
  }
}
