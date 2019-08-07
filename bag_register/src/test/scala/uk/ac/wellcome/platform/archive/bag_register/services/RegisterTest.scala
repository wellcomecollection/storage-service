package uk.ac.wellcome.platform.archive.bag_register.services

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  StorageManifestVHSFixture
}
import uk.ac.wellcome.platform.archive.common.generators.StorageSpaceGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.platform.archive.common.storage.services.{
  BadFetchLocationException,
  MemorySizeFinder,
  StorageManifestService
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

class RegisterTest
  extends FunSpec
    with Matchers
    with StorageManifestVHSFixture
    with RandomThings
    with StorageSpaceGenerators
    with StringNamespaceFixtures
    with TryValues {

  it("includes a user-facing message if the fetch.txt refers to the wrong namespace") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    implicit val typedStore: MemoryTypedStore[ObjectLocation, String] =
      new MemoryTypedStore[ObjectLocation, String]()

    val bagReader = new MemoryBagReader()

    val storageManifestService = new StorageManifestService(
      sizeFinder = new MemorySizeFinder(streamStore.memoryStore)
    )

    val space = createStorageSpace
    val version = randomInt(1, 15)

    val storageManifestDao = createStorageManifestDao()

    val register = new Register(
      bagReader = bagReader,
      storageManifestDao,
      storageManifestService = storageManifestService
    )

    val (bagObjects, bagRoot, _) =
      withNamespace { implicit namespace =>
        BagBuilder.createBagContentsWith(
          version = BagVersion(version)
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

    val result = register.update(
      bagRootLocation = bagRoot.copy(
        namespace = bagRoot.namespace + "_wrong"
      ),
      version = BagVersion(version),
      storageSpace = space
    )

    result.success.value shouldBe a[IngestFailed[_]]
    val ingestFailed = result.success.value.asInstanceOf[IngestFailed[_]]

    ingestFailed.e shouldBe a[BadFetchLocationException]
    ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
      """Fetch entry for data/[0-9A-Za-z/]+ refers to a file in the wrong namespace: [0-9A-Za-z/]+"""
  }
}
