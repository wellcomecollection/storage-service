package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.bag_tracker.models.{BagVersionEntry, BagVersionList}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

trait ListVersionsTestCases extends AnyFunSpec with EitherValues with ScalaFutures with BagTrackerClientTestBase with StorageManifestGenerators {
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
          val future = client.listVersionsOf(manifest.id, before = None)

          whenReady(future) {
            _.right.value shouldBe expectedList
          }
        }
      }
    }

    it("finds multiple versions of a bag") {
      true shouldBe false
    }

    it("filters to versions before a given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions for this bag ID") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions before the given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerUnknownListError] if the API has an unexpected error") {
      true shouldBe false
    }
  }
}
