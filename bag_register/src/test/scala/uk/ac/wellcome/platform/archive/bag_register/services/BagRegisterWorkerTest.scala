package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models._

class BagRegisterWorkerTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with BagInfoGenerators
    with BagRegisterFixtures
    with PayloadGenerators {

  it("handles a successful registration") {
    val createdAfterDate = Instant.now()
    val space = createStorageSpace
    val version = createBagVersion
    val dataFileCount = randomInt(1, 15)

    val ingests = new MemoryMessageSender()

    val storageManifestDao = createStorageManifestDao()

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = createRegisterBagWith(
        space = space,
        dataFileCount = dataFileCount,
        version = version
      )

      val primaryLocation = PrimaryS3ReplicaLocation(prefix = bagRoot)

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

      withBagRegisterWorker(
        ingests = ingests,
        storageManifestDao = storageManifestDao
      ) { worker =>
        val future = worker.processPayload(payload)

        whenReady(future) {
          _ shouldBe a[IngestCompleted[_]]
        }
      }

      val bagId = BagId(
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      val storageManifest =
        storageManifestDao.getLatest(bagId).right.value

      storageManifest.space shouldBe bagId.space
      storageManifest.info shouldBe bagInfo
      storageManifest.manifest.files should have size dataFileCount

      storageManifest.location shouldBe PrimaryS3StorageLocation(
        prefix = bagRoot
          .copy(
            keyPrefix = bagRoot.keyPrefix.stripSuffix(s"/$version")
          )
      )

      storageManifest.replicaLocations shouldBe empty

      storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

      assertBagRegisterSucceeded(
        ingestId = payload.ingestId,
        ingests = ingests
      )
    }
  }

  it("sends a notification of a registered bag") {
    val space = createStorageSpace
    val version = createBagVersion

    val registrationNotifications = new MemoryMessageSender()

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = createRegisterBagWith(
        space = space,
        version = version
      )

      val primaryLocation = PrimaryS3ReplicaLocation(prefix = bagRoot)

      val knownReplicas = KnownReplicas(
        location = primaryLocation,
        replicas = List.empty
      )

      val payload = createKnownReplicasPayloadWith(
        context = createPipelineContextWith(
          storageSpace = space,
          externalIdentifier = bagInfo.externalIdentifier
        ),
        version = version,
        knownReplicas = knownReplicas
      )

      withBagRegisterWorker(
        registrationNotifications = registrationNotifications
      ) { worker =>
        val future = worker.processPayload(payload)

        whenReady(future) {
          _ shouldBe a[IngestCompleted[_]]
        }
      }

      registrationNotifications
        .getMessages[BagRegistrationNotification]() shouldBe Seq(
        BagRegistrationNotification(
          space = space,
          externalIdentifier = bagInfo.externalIdentifier,
          version = version
        )
      )
    }
  }

  it("stores multiple versions of a bag") {
    val version1 = createBagVersion
    val version2 = version1.increment

    val storageManifestDao = createStorageManifestDao()

    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier

    withLocalS3Bucket { implicit bucket =>
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
        location = PrimaryS3ReplicaLocation(prefix = bagRoot1),
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
        location = PrimaryS3ReplicaLocation(prefix = bagRoot2),
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

      withBagRegisterWorker(storageManifestDao = storageManifestDao) { worker =>
        whenReady(worker.processPayload(payload1)) {
          _ shouldBe a[IngestCompleted[_]]
        }

        whenReady(worker.processPayload(payload2)) {
          _ shouldBe a[IngestCompleted[_]]
        }
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

  it("registers a bag with multiple locations") {
    val space = createStorageSpace
    val version = createBagVersion

    val storageManifestDao = createStorageManifestDao()

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = createRegisterBagWith(
        space = space,
        version = version
      )

      val primaryLocation = PrimaryS3ReplicaLocation(prefix = bagRoot)

      val replicas = collectionOf(min = 1) {
        SecondaryS3ReplicaLocation(
          prefix = bagRoot.copy(bucket = createBucketName)
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

      withBagRegisterWorker(storageManifestDao = storageManifestDao) { worker =>
        val future = worker.processPayload(payload)

        whenReady(future) {
          _ shouldBe a[IngestCompleted[_]]
        }
      }

      val storageManifest =
        storageManifestDao.getLatest(bagId).right.value

      storageManifest.location shouldBe PrimaryS3StorageLocation(
        bagRoot
          .copy(
            keyPrefix = bagRoot.keyPrefix.stripSuffix(s"/$version")
          )
      )

      storageManifest.replicaLocations shouldBe
        replicas.map { secondaryLocation =>
          val prefix = secondaryLocation.prefix

          SecondaryS3StorageLocation(
            prefix
              .copy(keyPrefix = prefix.keyPrefix.stripSuffix(s"/$version"))
          )
        }

      val tagManifestFiles =
        storageManifest.tagManifest.files
          .filter { _.name == "tagmanifest-sha256.txt" }

      tagManifestFiles should have size 1
    }
  }

  it("handles a failed registration") {
    val ingests = new MemoryMessageSender()
    val registrationNotifications = new MemoryMessageSender()

    // This registration will fail because when the register tries to read the
    // bag from the store, it won't find anything at the primary location
    // in this payload.
    val payload = createKnownReplicasPayload

    val future = withBagRegisterWorker(
      ingests = ingests,
      registrationNotifications = registrationNotifications
    ) {
      _.processPayload(payload)
    }

    whenReady(future) {
      _ shouldBe a[IngestFailed[_]]
    }

    assertBagRegisterFailed(
      ingestId = payload.ingestId,
      ingests = ingests
    )

    registrationNotifications.messages shouldBe empty
  }
}
