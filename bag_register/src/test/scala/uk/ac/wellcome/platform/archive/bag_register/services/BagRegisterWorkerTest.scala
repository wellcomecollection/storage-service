package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  PayloadGenerators,
  StorageLocationGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  KnownReplicas,
  PrimaryStorageLocation
}
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
    with StorageLocationGenerators
    with TryValues {

  describe("handling a successful registration") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    implicit val namespace: String = randomAlphanumeric

    val createdAfterDate = Instant.now()
    val space = createStorageSpace
    val version = createBagVersion
    val dataFileCount = randomInt(1, 15)

    val ingests = new MemoryMessageSender()

    val storageManifestDao = createStorageManifestDao()

    val (bagRoot, bagInfo) = createRegisterBagWith(
      space = space,
      dataFileCount = dataFileCount,
      version = version
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

    val result =
      withBagRegisterWorker(
        ingests = ingests,
        storageManifestDao = storageManifestDao
      ) {
        _.processMessage(payload)
      }

    it("returns an IngestCompleted") {
      result shouldBe a[Success[_]]
      result.success.value shouldBe a[IngestCompleted[_]]
    }

    it("stores the manifest in the dao") {
      val bagId = BagId(
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      val storageManifest =
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
    }

    it("updates the ingests monitor") {
      assertBagRegisterSucceeded(
        ingestId = payload.ingestId,
        ingests = ingests
      )
    }
  }

  describe("stores multiple versions of a bag") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val version1 = createBagVersion
    val version2 = version1.increment

    val storageManifestDao = createStorageManifestDao()

    implicit val namespace: String = randomAlphanumeric

    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier

    val (bagRoot1, bagInfo1) = createRegisterBagWith(
      externalIdentifier = externalIdentifier,
      space = space,
      version = version1
    )

    val (bagRoot2, _) = createRegisterBagWith(
      externalIdentifier = externalIdentifier,
      space = space,
      version = version2
    )

    val knownReplicas1 = KnownReplicas(
      location = createPrimaryLocationWith(prefix = bagRoot1),
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
      location = createPrimaryLocationWith(prefix = bagRoot2),
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

    val results =
      withBagRegisterWorker(storageManifestDao = storageManifestDao) { worker =>
        Seq(payload1, payload2).map { worker.processMessage }
      }

    it("returns an IngestCompleted") {
      results.foreach { result =>
        result shouldBe a[Success[_]]
        result.success.value shouldBe a[IngestCompleted[_]]
      }
    }

    it("stores multiple versions in the dao") {
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

  it("registers a bag with multiple locations") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    implicit val namespace: String = randomAlphanumeric

    val space = createStorageSpace
    val version = createBagVersion

    val storageManifestDao = createStorageManifestDao()

    val (bagRoot, bagInfo) = createRegisterBagWith(
      space = space,
      version = version
    )

    val primaryLocation = createPrimaryLocationWith(
      prefix = bagRoot
    )

    val replicas = collectionOf(min = 1) {
      createSecondaryLocationWith(
        prefix = bagRoot.copy(namespace = randomAlphanumeric)
      )
    }

    val knownReplicas = KnownReplicas(
      location = primaryLocation,
      replicas = replicas
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

    val result =
      withBagRegisterWorker(storageManifestDao = storageManifestDao) {
        _.processMessage(payload)
      }

    it("returns an IngestCompleted") {
      result shouldBe a[Success[_]]
      result.success.value shouldBe a[IngestCompleted[_]]
    }

    it("stores the manifest in the dao") {
      val storageManifest =
        storageManifestDao.getLatest(bagId).right.value

      storageManifest.location shouldBe primaryLocation.copy(
        prefix = bagRoot
          .copy(
            path = bagRoot.path.stripSuffix(s"/$version")
          )
      )

      storageManifest.replicaLocations shouldBe
        replicas.map { secondaryLocation =>
          val prefix = secondaryLocation.prefix

          secondaryLocation.copy(
            prefix = prefix
              .copy(path = prefix.path.stripSuffix(s"/$version"))
          )
        }
    }
  }

  describe("handles a failed registration") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val ingests = new MemoryMessageSender()

    // This registration will fail because when the register tries to read the
    // bag from the store, it won't find anything at the primary location
    // in this payload.
    val payload = createKnownReplicasPayload

    val result =
      withBagRegisterWorker(ingests = ingests) {
        _.processMessage(payload)
      }

    it("returns an IngestFailed") {
      result shouldBe a[Success[_]]
      result.success.value shouldBe a[IngestFailed[_]]
    }

    it("updates the ingests monitor") {
      assertBagRegisterFailed(
        ingestId = payload.ingestId,
        ingests = ingests
      )
    }
  }
}
