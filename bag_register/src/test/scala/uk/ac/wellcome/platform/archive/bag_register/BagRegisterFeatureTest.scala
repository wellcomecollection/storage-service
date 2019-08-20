package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{KnownReplicas, PrimaryStorageLocation}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class BagRegisterFeatureTest
    extends FunSpec
    with Matchers
    with BagRegisterFixtures
    with PayloadGenerators
    with Eventually
    with IntegrationPatience {

  it("sends an update if it registers a bag") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    withBagRegisterWorker {
      case (_, storageManifestDao, ingests, _, queuePair) =>
        val createdAfterDate = Instant.now()
        val space = createStorageSpace
        val version = createBagVersion
        val dataFileCount = randomInt(1, 15)
        val externalIdentifier = createExternalIdentifier

        val bagId = BagId(
          space = space,
          externalIdentifier = externalIdentifier
        )

        withNamespace { implicit namespace =>
          withRegisterBag(
            externalIdentifier,
            space,
            version,
            dataFileCount = dataFileCount
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

              sendNotificationToSQS(queuePair.queue, payload)

              eventually {
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

                assertQueueEmpty(queuePair.queue)
              }
          }
        }
    }
  }

  it("sends a failed update and discards the work on error") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    withBagRegisterWorker {
      case (_, _, ingests, _, queuePair) =>
        val payload = createKnownReplicasPayload

        sendNotificationToSQS(queuePair.queue, payload)

        eventually {
          assertBagRegisterFailed(
            ingestId = payload.ingestId,
            ingests = ingests
          )
        }

        assertQueueEmpty(queuePair.queue)
        assertQueueEmpty(queuePair.dlq)
    }
  }
}
