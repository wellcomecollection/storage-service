package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagFetchEntry, BagPath}
import uk.ac.wellcome.platform.archive.common.generators.BagGenerators
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class StorageManifestServiceTest
  extends FunSpec
    with Matchers
    with BagGenerators
    with ObjectLocationGenerators
    with TryValues {

  it("rejects a bag if the root location is not a versioned directory") {
    val bagRootLocation = createObjectLocation
    val version = randomInt(1, 10)

    assertIsError(replicaRootLocation = bagRootLocation, version = version) {
      _ shouldBe s"Malformed bag root: $bagRootLocation (expected suffix /v$version)"
    }
  }

  it("rejects a bag if the versioned directory is wrong") {
    val version = randomInt(1, 10)
    val bagRootLocation = createObjectLocation.join(s"/v${version + 1}")

    assertIsError(replicaRootLocation = bagRootLocation, version = version) {
      _ shouldBe s"Malformed bag root: $bagRootLocation (expected suffix /v$version)"
    }
  }

  it("rejects a bag if the fetch.txt refers to files that aren't in the manifest") {
    val fetchEntries = Seq(
      BagFetchEntry(
        uri = new URI("https://example.org/file1.txt"),
        length = None,
        path = BagPath(randomAlphanumeric)
      )
    )

    val bag = createBagWith(
      fetchEntries = fetchEntries
    )

    assertIsError(bag = bag) { msg =>
      msg should startWith("Unable to resolve fetch entries:")
      msg should include(s"Fetch entry refers to a path that isn't in the bag: ${fetchEntries.head}")
    }
  }

  // TEST: Creates correct root location
  // TEST: Puts that location in the manifest with s3-whatever
  // TEST: If there are no fetch entries, puts all entries with correct versioned path
  // TEST: If the fetch entry is in wrong namespace, reject
  // TEST: If the fetch entry is in the wrong path, reject
  // TEST: Applies the right version prefix to fetch files
  // TEST: Correct storage space
  // TEST: Correct bagInfo
  // TEST: Correct version
  // TEST: Recent createdDate

  private def assertIsError(
    bag: Bag = createBag,
    replicaRootLocation: ObjectLocation = createObjectLocation.join("/v1"),
    version: Int = 1
  )(assertMessage: String => Assertion): Assertion = {
    val result = StorageManifestService.createManifest(
      bag = bag,
      replicaRootLocation = replicaRootLocation,
      version = version
    )

    result.failure.exception shouldBe a[StorageManifestException]
    assertMessage(result.failure.exception.getMessage)
  }
}
