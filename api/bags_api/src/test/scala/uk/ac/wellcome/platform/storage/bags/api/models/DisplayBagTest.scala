package uk.ac.wellcome.platform.storage.bags.api.models

import java.net.URL

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

class DisplayBagTest extends FunSpec with Matchers with StorageManifestGenerators {
  val contextUrl = new URL("http://api.wellcomecollection.org/storage/v1/context.json")

  it("uses the access and archive locations for the 'locations' field") {
    val storageManifest = createStorageManifestWith(
      accessLocation = createObjectLocation,
      archiveLocations = List(
        createObjectLocation,
        createObjectLocation,
        createObjectLocation
      )
    )

    val displayBag = DisplayBag(
      storageManifest = storageManifest,
      contextUrl = contextUrl
    )

    val expectedLocations = storageManifest.accessLocation +: storageManifest.archiveLocations
    displayBag.locations.map { _.toStorageLocation } shouldBe expectedLocations
  }
}
