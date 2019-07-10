package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
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

    assertIsError(bagRootLocation = bagRootLocation, version = version) {
      _ shouldBe s"Malformed bag root: $bagRootLocation (expected suffix /v$version)"
    }
  }

  private def assertIsError(
    bag: Bag = createBag,
    bagRootLocation: ObjectLocation,
    version: Int
  )(assertMessage: String => Assertion): Assertion = {
    val result = StorageManifestService.createManifest(
      bag = bag,
      replicaRootLocation = bagRootLocation,
      version = version
    )

    result.failure.exception shouldBe a[StorageManifestException]
    assertMessage(result.failure.exception.getMessage)
  }
}
