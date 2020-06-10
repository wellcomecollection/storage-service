package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

class TagRulesTest extends AnyFunSpec with Matchers with StorageManifestGenerators {
  describe("Content-Type=application/mxf for MXF video masters") {
    describe("it applies a tag") {
      it("for MXFs in the digitised space") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(createStorageManifestFileWith(name = "b1234.mxf"))
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          BagPath("b1234.mxf") -> Map("Content-Type" -> "application/mxf")
        )
      }

      it("whose file extension is uppercase") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(createStorageManifestFileWith(name = "b1234.MXF"))
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          BagPath("b1234.MXF") -> Map("Content-Type" -> "application/mxf")
        )
      }
    }

    describe("it does not apply a tag") {
      it("to non-MXF files") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(createStorageManifestFileWith(name = "b1234.mp4"))
        )

        TagRules.chooseTags(manifest) shouldBe empty
      }

      it("to MXF files outside the digitised space") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("born-digital"),
          files = Seq(createStorageManifestFileWith(name = "b1234.mxf"))
        )

        TagRules.chooseTags(manifest) shouldBe empty
      }
    }
  }
}
