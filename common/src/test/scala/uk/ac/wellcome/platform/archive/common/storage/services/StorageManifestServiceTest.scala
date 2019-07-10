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
    assertIsError(bagRootLocation = createObjectLocation) { msg =>
      msg should contain(s"Malformed bag root: $bagRootLocation (not a versioned directory)")
    }
  }

  private def assertIsError(
    bag: Bag = createBag,
    bagRootLocation: ObjectLocation,
    version: Int = randomInt(1, 10)
  )(assertMessage: String => Assertion): Assertion = {
    val result = StorageManifestService.createManifest(
      bag = bag,
      bagRootLocation = bagRootLocation,
      version = version
    )

    result.failure.exception shouldBe a[StorageManifestException]
    assertMessage(result.failure.exception.getMessage)
  }
}
