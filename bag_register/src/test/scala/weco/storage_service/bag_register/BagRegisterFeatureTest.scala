package weco.storage_service.bag_register

import java.time.Instant
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.QueuePair
import weco.messaging.memory.MemoryMessageSender
import weco.storage.Version
import weco.storage.maxima.memory.MemoryMaxima
import weco.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import weco.storage_service.bag_register.fixtures.BagRegisterFixtures
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage_service.generators.PayloadGenerators
import weco.storage_service.ingests.models.{Ingest, IngestStatusUpdate, IngestUpdate}
import weco.storage_service.storage.models._

import scala.concurrent.duration._

class BagRegisterFeatureTest
    extends AnyFunSpec
    with Matchers
    with BagRegisterFixtures
    with PayloadGenerators
    with Eventually
    with IntegrationPatience {

  it("sends an update if it registers a bag") {
    val ingests = new MemoryMessageSender()

    val storageManifestDao = createStorageManifestDao()

    val createdAfterDate = Instant.now()
    val space = createStorageSpace
    val version = createBagVersion
    val payloadFileCount = randomInt(1, 15)

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = storeS3BagWith(
        space = space,
        version = version,
        payloadFileCount = payloadFileCount
      )

      val bagId = BagId(
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      val primaryLocation = PrimaryS3ReplicaLocation(
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
              storageManifestDao.getLatest(bagId).value

            storageManifest.space shouldBe bagId.space
            storageManifest.info shouldBe bagInfo
            storageManifest.manifest.files should have size payloadFileCount

            storageManifest.location shouldBe PrimaryS3StorageLocation(
              prefix = bagRoot
                .copy(
                  keyPrefix = bagRoot.keyPrefix.stripSuffix(s"/$version")
                )
            )

            storageManifest.replicaLocations shouldBe empty

            storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

            assertBagRegisterSucceeded(ingests)

            assertQueueEmpty(queue)
          }
        }
      }
    }
  }

  it("can receive the same update twice") {
    val ingests = new MemoryMessageSender()

    val store = new MemoryStore[Version[BagId, Int], StorageManifest](initialEntries = Map())
      with MemoryMaxima[BagId, StorageManifest]

    val storageManifestDao =
      new MemoryStorageManifestDao(
        new MemoryVersionedStore[BagId, StorageManifest](store)
      )

    val space = createStorageSpace
    val version = createBagVersion

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, _) = storeS3BagWith(
        space = space,
        version = version
      )

      val knownReplicas = KnownReplicas(
        location = PrimaryS3ReplicaLocation(prefix = bagRoot),
        replicas = List.empty
      )

      val payload = createKnownReplicasPayloadWith(
        context = createPipelineContextWith(
          storageSpace = space
        ),
        version = version,
        knownReplicas = knownReplicas
      )

      withLocalSqsQueuePair(visibilityTimeout = 1 second) { case QueuePair(queue, dlq) =>
        withBagRegisterWorker(
          queue = queue,
          ingests = ingests,
          storageManifestDao = storageManifestDao
        ) { _ =>
          sendNotificationToSQS(queue, payload)
          sendNotificationToSQS(queue, payload)

          eventually {
            store.entries should have size 1

            assertQueueEmpty(queue)
            assertQueueEmpty(dlq)

            // (started + succeeded) × 2 = 4 events
            ingests.messages should have size 4
            ingests.getMessages[IngestUpdate]
              .collect { case IngestStatusUpdate(_, status, _) => status} shouldBe List(Ingest.Succeeded, Ingest.Succeeded)
          }
        }
      }
    }
  }

  it("handles a failed registration") {
    val ingests = new MemoryMessageSender()

    // This registration will fail because when the register tries to read the
    // bag from the store, it won't find anything at the primary location
    // in this payload.
    val payload = createKnownReplicasPayload

    withLocalSqsQueuePair() {
      case QueuePair(queue, dlq) =>
        withBagRegisterWorker(queue = queue, ingests = ingests) { _ =>
          sendNotificationToSQS(queue, payload)

          eventually {
            assertBagRegisterFailed(ingests)

            assertQueueEmpty(queue)
            assertQueueEmpty(dlq)
          }
        }
    }
  }
}
