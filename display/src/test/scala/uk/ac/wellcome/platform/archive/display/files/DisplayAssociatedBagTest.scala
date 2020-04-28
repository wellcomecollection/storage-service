package uk.ac.wellcome.platform.archive.display.files

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

class DisplayAssociatedBagTest
    extends FunSpec
    with Matchers
    with StorageManifestGenerators {
  it("creates an associated bag from a storage manifest") {
    val manifest = createStorageManifest

    val displayBag = DisplayAssociatedBag(manifest)

    displayBag.space shouldBe manifest.space.toString
    displayBag.externalIdentifier shouldBe manifest.info.externalIdentifier.toString
    displayBag.version shouldBe manifest.version.toString
    displayBag.createdDate shouldBe manifest.createdDate.toString
  }
}
