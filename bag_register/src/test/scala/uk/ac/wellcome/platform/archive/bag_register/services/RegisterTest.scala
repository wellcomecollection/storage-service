package uk.ac.wellcome.platform.archive.bag_register.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
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
  StorageManifestService
}
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{
  MemoryStreamStore,
  NewMemoryTypedStore
}

import scala.concurrent.ExecutionContext.Implicits.global

class RegisterTest
    extends AnyFunSpec
    with Matchers
    with BagRegisterFixtures
    with StorageSpaceGenerators
    with StorageLocationGenerators
    with StringNamespaceFixtures
    with BagTrackerFixtures
    with ScalaFutures
    with IntegrationPatience {

  it("registers a bag with primary and secondary locations") {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val bagReader = new MemoryBagReader()

    val storageManifestService = new StorageManifestService(
      sizeFinder = new MemorySizeFinder[ObjectLocation](streamStore.memoryStore),
      toIdent = identity
    )

    val storageManifestDao = createStorageManifestDao()

    val space = createStorageSpace
    val version = createBagVersion

    val (bagRoot, bagInfo) = createRegisterBagWith(
      space = space,
      version = version
    )

    val primaryLocation = createPrimaryLocationWith(
      prefix = bagRoot.toObjectLocationPrefix
    )

    val replicas = collectionOf(min = 1) {
      createSecondaryLocationWith(
        prefix =
          bagRoot.copy(namespace = randomAlphanumeric).toObjectLocationPrefix
      )
    }

    val ingestId = createIngestID

    withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
      val register = new Register(
        bagReader = bagReader,
        bagTrackerClient = bagTrackerClient,
        storageManifestService = storageManifestService
      )

      val future = register.update(
        ingestId = ingestId,
        location = primaryLocation,
        replicas = replicas,
        version = version,
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      whenReady(future) { result =>
        result shouldBe a[IngestCompleted[_]]

        val summary = result.asInstanceOf[IngestCompleted[_]].summary
        summary.asInstanceOf[RegistrationSummary].ingestId shouldBe ingestId
      }
    }

    // Check it stores all the locations on the bag.
    val bagId = BagId(
      space = space,
      externalIdentifier = bagInfo.externalIdentifier
    )

    val manifest =
      storageManifestDao.getLatest(id = bagId).right.value

    manifest.location shouldBe primaryLocation.copy(
      prefix = bagRoot
        .copy(
          pathPrefix = bagRoot.pathPrefix.stripSuffix(s"/$version")
        )
        .toObjectLocationPrefix
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

  it(
    "includes a user-facing message if the fetch.txt refers to the wrong namespace"
  ) {
    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    implicit val typedStore: NewMemoryTypedStore[String] =
      new NewMemoryTypedStore[String]()

    val bagReader = new MemoryBagReader()

    val storageManifestService = new StorageManifestService(
      sizeFinder = new MemorySizeFinder[ObjectLocation](streamStore.memoryStore),
      toIdent = identity
    )

    val space = createStorageSpace
    val version = createBagVersion

    val storageManifestDao = createStorageManifestDao()

    val (bagObjects, bagRoot, bagInfo) =
      withNamespace { implicit namespace =>
        createBagContentsWith(
          version = version
        )
      }

    // Actually upload the bag objects into a different namespace,
    // so the entries in the fetch.txt will be wrong.
    val badBagObjects = bagObjects.map {
      case (objLocation, contents) =>
        objLocation.copy(namespace = objLocation.namespace + "_wrong") -> contents
    }

    uploadBagObjects(badBagObjects)

    val location = createPrimaryLocationWith(
      prefix = bagRoot
        .copy(
          namespace = bagRoot.namespace + "_wrong"
        )
        .toObjectLocationPrefix
    )

    withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
      val register = new Register(
        bagReader = bagReader,
        bagTrackerClient = bagTrackerClient,
        storageManifestService = storageManifestService
      )

      val future = register.update(
        ingestId = createIngestID,
        location = location,
        replicas = Seq.empty,
        version = version,
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      whenReady(future) { result =>
        result shouldBe a[IngestFailed[_]]

        val ingestFailed = result.asInstanceOf[IngestFailed[_]]
        ingestFailed.e shouldBe a[BadFetchLocationException]
        ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
          """Fetch entry for data/[0-9A-Za-z/]+ refers to a file in the wrong namespace: [0-9A-Za-z/]+"""
      }
    }
  }
}
