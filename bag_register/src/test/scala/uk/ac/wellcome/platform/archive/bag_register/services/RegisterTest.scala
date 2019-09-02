package uk.ac.wellcome.platform.archive.bag_register.services

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.{
  StorageLocationGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed
}
import uk.ac.wellcome.platform.archive.common.storage.services.{
  BadFetchLocationException,
  MemorySizeFinder,
  StorageManifestService
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

class RegisterTest
    extends FunSpec
    with Matchers
    with BagRegisterFixtures
    with StorageSpaceGenerators
    with StorageLocationGenerators
    with StringNamespaceFixtures
    with TryValues {

  describe("registering a bag with primary and secondary locations") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val bagReader = new MemoryBagReader()

    val storageManifestService = new StorageManifestService(
      sizeFinder = new MemorySizeFinder(streamStore.memoryStore)
    )

    val storageManifestDao = createStorageManifestDao()

    val register = new Register(
      bagReader = bagReader,
      storageManifestDao,
      storageManifestService = storageManifestService
    )

    val space = createStorageSpace
    val version = createBagVersion

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

    val ingestId = createIngestID

    val result = register.update(
      ingestId = ingestId,
      location = primaryLocation,
      replicas = replicas,
      version = version,
      space = space
    )

    it("succeeds") {
      result.success.value shouldBe a[IngestCompleted[_]]
    }

    it("uses the ingest ID in the summary") {
      val summary = result.success.value.asInstanceOf[IngestCompleted[_]].summary
      summary.asInstanceOf[RegistrationSummary].ingestId shouldBe ingestId
    }

    it("stores all the locations in the dao") {
      val bagId = BagId(
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      val manifest =
        storageManifestDao.getLatest(id = bagId).right.value

      manifest.location shouldBe primaryLocation.copy(
        prefix = bagRoot
          .copy(
            path = bagRoot.path.stripSuffix(s"/$version")
          )
      )

      manifest.replicaLocations shouldBe
        replicas.map { secondaryLocation =>
          val prefix = secondaryLocation.prefix

          secondaryLocation.copy(
            prefix = prefix
              .copy(path = prefix.path.stripSuffix(s"/$version"))
          )
        }
    }
  }

  it(
    "includes a user-facing message if the fetch.txt refers to the wrong namespace"
  ) {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    implicit val typedStore: MemoryTypedStore[ObjectLocation, String] =
      new MemoryTypedStore[ObjectLocation, String]()

    val bagReader = new MemoryBagReader()

    val storageManifestService = new StorageManifestService(
      sizeFinder = new MemorySizeFinder(streamStore.memoryStore)
    )

    val space = createStorageSpace
    val version = createBagVersion

    val storageManifestDao = createStorageManifestDao()

    val register = new Register(
      bagReader = bagReader,
      storageManifestDao,
      storageManifestService = storageManifestService
    )

    val (bagObjects, bagRoot, _) =
      withNamespace { implicit namespace =>
        BagBuilder.createBagContentsWith(
          version = version
        )
      }

    // Actually upload the bag objects into a different namespace,
    // so the entries in the fetch.txt will be wrong.
    val badBagObjects = bagObjects.map { bagObject =>
      bagObject.copy(
        location = bagObject.location.copy(
          namespace = bagObject.location.namespace + "_wrong"
        )
      )
    }
    BagBuilder.uploadBagObjects(badBagObjects)

    val location = createPrimaryLocationWith(
      prefix = bagRoot
        .copy(
          namespace = bagRoot.namespace + "_wrong"
        )
    )

    val result = register.update(
      ingestId = createIngestID,
      location = location,
      replicas = Seq.empty,
      version = version,
      space = space
    )

    result.success.value shouldBe a[IngestFailed[_]]
    val ingestFailed = result.success.value.asInstanceOf[IngestFailed[_]]

    ingestFailed.e shouldBe a[BadFetchLocationException]
    ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
      """Fetch entry for data/[0-9A-Za-z/]+ refers to a file in the wrong namespace: [0-9A-Za-z/]+"""
  }
}
