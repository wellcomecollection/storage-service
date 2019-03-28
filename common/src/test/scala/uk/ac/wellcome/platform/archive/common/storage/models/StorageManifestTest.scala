package uk.ac.wellcome.platform.archive.common.storage.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

class StorageManifestTest extends FunSpec with Matchers with StorageManifestGenerators {
  it("combines the archive and the access locations in the 'locations' field") {
    val storageManifest = createStorageManifestWith(
      accessLocation = createObjectLocation,
      archiveLocations = List(
        createObjectLocation,
        createObjectLocation,
        createObjectLocation
      )
    )

    storageManifest.locations shouldBe storageManifest.accessLocation +: storageManifest.archiveLocations
  }
}
