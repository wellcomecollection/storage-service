package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.bag_tracker.models.{BagVersionEntry, BagVersionList}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, StorageManifestGenerators}

trait ListVersionsTestCases extends AnyFunSpec with EitherValues with ScalaFutures with BagTrackerClientTestBase with BagIdGenerators with StorageManifestGenerators {
  describe("listVersionsOf") {
    it("finds a single version of a bag") {
      val manifest = createStorageManifest

      val expectedList = BagVersionList(
        id = manifest.id,
        versions = Seq(
          BagVersionEntry(version = manifest.version, createdDate = manifest.createdDate)
        )
      )

      withApi(initialManifests = Seq(manifest)) { _ =>
        withClient { client =>
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
          BagVersionEntry(version = manifest.version, createdDate = manifest.createdDate)
        }

      withApi(initialManifests = manifests) { _ =>
        withClient { client =>
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
            BagVersionEntry(version = manifest.version, createdDate = manifest.createdDate)
          }

      withApi(initialManifests = manifests) { _ =>
        withClient { client =>
          val future = client.listVersionsOf(bagId, maybeBefore = Some(BagVersion(3)))

          whenReady(future) { result =>
            val bagVersionList = result.right.value

            bagVersionList.id shouldBe bagId
            bagVersionList.versions should contain theSameElementsAs expectedEntries
          }
        }
      }
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions for this bag ID") {
      val bagId = createBagId

      withApi(initialManifests = Seq.empty) { _ =>
        withClient { client =>
          val future = client.listVersionsOf(bagId, maybeBefore = None)

          whenReady(future) {
            _.left.value shouldBe BagTrackerNotFoundListError(bagId, maybeBefore = None)
          }
        }
      }
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions before the given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerUnknownListError] if the API has an unexpected error") {
      true shouldBe false
    }
  }
}
