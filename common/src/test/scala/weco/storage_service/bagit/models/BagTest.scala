package weco.storage_service.bagit.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.checksum.SHA256
import weco.storage_service.generators.BagInfoGenerators

class BagTest extends AnyFunSpec with Matchers with BagInfoGenerators {
  describe("has backwards compatibility") {
    it("keeps the payload manifest") {
      val entries = (1 to randomInt(from = 1, to = 10))
        .map { _ => createBagPath -> randomChecksumValue }
        .toMap

      val manifest = PayloadManifest(checksumAlgorithm = SHA256, entries = entries)

      val bag = Bag(
        info = createBagInfo,
        manifest = manifest,
        tagManifest = TagManifest(checksumAlgorithm = SHA256, entries = Map()),
        fetch = None
      )

      bag.manifest shouldBe manifest
    }

    it("keeps the tag manifest") {
      val entries = (1 to randomInt(from = 1, to = 10))
        .map { _ => createBagPath -> randomChecksumValue }
        .toMap

      val tagManifest = TagManifest(checksumAlgorithm = SHA256, entries = entries)

      val bag = Bag(
        info = createBagInfo,
        manifest = PayloadManifest(checksumAlgorithm = SHA256, entries = Map()),
        tagManifest = tagManifest,
        fetch = None
      )

      bag.tagManifest shouldBe tagManifest
    }
  }
}
