package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.ac.wellcome.platform.archive.bag_tracker.models.{
  BagVersionEntry,
  BagVersionList
}
import uk.ac.wellcome.platform.archive.bag_tracker.storage.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.{ReadError, RetryableError, StoreReadError}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait ListVersionsTestCases
    extends AnyFunSpec
    with EitherValues
    with ScalaFutures
    with BagTrackerClientTestBase
    with BagIdGenerators
    with StorageManifestGenerators
    with TableDrivenPropertyChecks {
  val unusualIdentifiers: TableFor1[String]

  describe("listVersionsOf") {
    it("finds a single version of a bag") {
      val manifest = createStorageManifest

      val expectedList = BagVersionList(
        id = manifest.id,
        versions = Seq(
          BagVersionEntry(
            version = manifest.version,
            createdDate = manifest.createdDate
          )
        )
      )

      withApi(initialManifests = Seq(manifest)) { _ =>
        withClient(trackerHost) { client =>
          val future = client.listVersionsOf(manifest.id, maybeBefore = None)

          whenReady(future) {
            _.right.value shouldBe expectedList
          }
        }
      }
    }

    it("finds multiple versions of a bag") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      val expectedEntries =
        manifests.map { manifest =>
          BagVersionEntry(
            version = manifest.version,
            createdDate = manifest.createdDate
          )
        }

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future = client.listVersionsOf(bagId, maybeBefore = None)

          whenReady(future) { result =>
            val bagVersionList = result.right.value

            bagVersionList.id shouldBe bagId
            bagVersionList.versions should contain theSameElementsAs expectedEntries
          }
        }
      }
    }

    it("filters to versions before a given version") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      val expectedEntries =
        manifests
          .filter { _.version.underlying < 3 }
          .map { manifest =>
            BagVersionEntry(
              version = manifest.version,
              createdDate = manifest.createdDate
            )
          }

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future =
            client.listVersionsOf(bagId, maybeBefore = Some(BagVersion(3)))

          whenReady(future) { result =>
            val bagVersionList = result.right.value

            bagVersionList.id shouldBe bagId
            bagVersionList.versions should contain theSameElementsAs expectedEntries
          }
        }
      }
    }

    it("lists versions of a bag with spaces in the identifier") {
      forAll(unusualIdentifiers) { identifier =>
        val manifest = createStorageManifestWith(
          bagInfo = createBagInfoWith(
            externalIdentifier = ExternalIdentifier(identifier)
          )
        )

        val expectedList = BagVersionList(
          id = manifest.id,
          versions = Seq(
            BagVersionEntry(
              version = manifest.version,
              createdDate = manifest.createdDate
            )
          )
        )

        withApi(initialManifests = Seq(manifest)) { _ =>
          withClient(trackerHost) { client =>
            whenReady(
              client.listVersionsOf(bagId = manifest.id, maybeBefore = None)
            ) {
              _.right.value shouldBe expectedList
            }
          }
        }
      }
    }

    it(
      "returns Left[BagTrackerNotFoundError] if there are no versions for this bag ID"
    ) {
      val bagId = createBagId

      withApi(initialManifests = Seq.empty) { _ =>
        withClient(trackerHost) { client =>
          val future = client.listVersionsOf(bagId, maybeBefore = None)

          whenReady(future) {
            _.left.value shouldBe BagTrackerNotFoundError()
          }
        }
      }
    }

    it(
      "returns Left[BagTrackerNotFoundError] if there are no versions before the given version"
    ) {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (5 to 10).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future =
            client.listVersionsOf(bagId, maybeBefore = Some(BagVersion(4)))

          whenReady(future) {
            _.left.value shouldBe BagTrackerNotFoundError()
          }
        }
      }
    }

    it(
      "returns Left[BagTrackerUnknownListError] if the API has an unexpected error"
    ) {
      val versionedStore =
        MemoryVersionedStore[BagId, StorageManifest](initialEntries = Map.empty)

      val brokenDao = new MemoryStorageManifestDao(versionedStore) {
        override def listVersions(
          bagId: BagId,
          before: Option[BagVersion]
        ): Either[ReadError, Seq[StorageManifest]] =
          Left(StoreReadError(new Throwable("BOOM!")))
      }

      withApi(brokenDao) { _ =>
        withClient(trackerHost) { client =>
          val future = client.listVersionsOf(createBagId, maybeBefore = None)

          whenReady(future) {
            _.left.value shouldBe a[BagTrackerUnknownListError]
          }
        }
      }
    }

    it("returns a retryable error if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future = client.listVersionsOf(createBagId, maybeBefore = None)

          whenReady(future) { result =>
            val err = result.left.value
            err shouldBe a[BagTrackerUnknownListError]
            err shouldBe a[RetryableError]
          }
        }
      }
    }
  }
}
