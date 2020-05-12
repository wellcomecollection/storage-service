package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.{
  PayloadGenerators,
  StorageLocationGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  KnownReplicas,
  PrimaryStorageLocation,
  StorageManifest
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class BagRegisterFeatureTest
    extends AnyFunSpec
    with Matchers
    with BagRegisterFixtures
    with PayloadGenerators
    with Eventually
    with StorageLocationGenerators
    with IntegrationPatience {

  it("sends an update if it registers a bag") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val ingests = new MemoryMessageSender()

    val storageManifestDao = createStorageManifestDao()

    val createdAfterDate = Instant.now()
    val space = createStorageSpace
    val version = createBagVersion
    val dataFileCount = randomInt(1, 15)

    val (bagRoot, bagInfo) = createRegisterBagWith(
      space = space,
      version = version,
      dataFileCount = dataFileCount
    )

    val bagId = BagId(
      space = space,
      externalIdentifier = bagInfo.externalIdentifier
    )

    val primaryLocation = createPrimaryLocationWith(
      prefix = bagRoot
    )

    val knownReplicas = KnownReplicas(
      location = primaryLocation,
      replicas = List.empty
    )

    val payload = createKnownReplicasPayloadWith(
      context = createPipelineContextWith(
        storageSpace = space
      ),
      version = version,
      knownReplicas = knownReplicas
    )

    withLocalSqsQueue() { queue =>
      withBagRegisterWorker(
        queue = queue,
        ingests = ingests,
        storageManifestDao = storageManifestDao
      ) { _ =>
        sendNotificationToSQS(queue, payload)

        eventually {
          val storageManifest: StorageManifest =
            storageManifestDao.getLatest(bagId).right.value

          storageManifest.space shouldBe bagId.space
          storageManifest.info shouldBe bagInfo
          storageManifest.manifest.files should have size dataFileCount

          storageManifest.location shouldBe PrimaryStorageLocation(
            provider = primaryLocation.provider,
            prefix = bagRoot
              .copy(
                path = bagRoot.path.stripSuffix(s"/$version")
              )
          )

          storageManifest.replicaLocations shouldBe empty

          storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

          assertBagRegisterSucceeded(
            ingestId = payload.ingestId,
            ingests = ingests
          )

          assertQueueEmpty(queue)
        }
      }
    }
  }

  it("handles a failed registration") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val ingests = new MemoryMessageSender()

    // This registration will fail because when the register tries to read the
    // bag from the store, it won't find anything at the primary location
    // in this payload.
    val payload = createKnownReplicasPayload

    withLocalSqsQueuePair(visibilityTimeout = 5) {
      case QueuePair(queue, dlq) =>
        withBagRegisterWorker(queue = queue, ingests = ingests) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertBagRegisterFailed(
              ingestId = payload.ingestId,
              ingests = ingests
            )

            assertQueueEmpty(queue)
            assertQueueEmpty(dlq)
          }
        }
    }
  }
}
