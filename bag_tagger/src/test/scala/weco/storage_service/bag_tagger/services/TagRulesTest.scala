package weco.storage_service.bag_tagger.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.generators.StorageManifestGenerators
import weco.storage_service.storage.models.StorageSpace

class TagRulesTest
    extends AnyFunSpec
    with Matchers
    with StorageManifestGenerators {
  describe("Content-Type=application/mxf for MXF video masters") {
    describe("it applies a tag") {
      it("for MXFs in the digitised space") {
        val mxfFile = createStorageManifestFileWith(name = "b1234.mxf")

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(mxfFile)
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          mxfFile -> Map("Content-Type" -> "application/mxf")
        )
      }

      it("whose file extension is uppercase") {
        val mxfFile = createStorageManifestFileWith(name = "b1234.MXF")

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(mxfFile)
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          mxfFile -> Map("Content-Type" -> "application/mxf")
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

  describe("Content-Type=application/tif for TIFF manuscripts masters") {
    describe("it applies a tag") {
      it("for TIFFs in the digitised space") {
        val tifFile = createStorageManifestFileWith(name = "b1842700_0001.tif")

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(tifFile)
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          tifFile -> Map("Content-Type" -> "image/tiff")
        )
      }

      it("for TIFFs that use two 'f's in the extension") {
        val tiffFile =
          createStorageManifestFileWith(name = "b1842700_0001.tiff")

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(tiffFile)
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          tiffFile -> Map("Content-Type" -> "image/tiff")
        )
      }

      it("whose file extension is uppercase") {
        val tifFile = createStorageManifestFileWith(name = "b1842700_0001.TIF")

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(tifFile)
        )

        TagRules.chooseTags(manifest) shouldBe Map(
          tifFile -> Map("Content-Type" -> "image/tiff")
        )
      }
    }

    describe("it does not apply a tag") {
      it("to non-TIFF files") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          files = Seq(createStorageManifestFileWith(name = "b1842700_0001.jp2"))
        )

        TagRules.chooseTags(manifest) shouldBe empty
      }

      it("to TIFF files outside the digitised space") {
        val manifest = createStorageManifestWith(
          space = StorageSpace("born-digital"),
          files = Seq(createStorageManifestFileWith(name = "330-19_19a.tif"))
        )

        TagRules.chooseTags(manifest) shouldBe empty
      }
    }
  }
}
