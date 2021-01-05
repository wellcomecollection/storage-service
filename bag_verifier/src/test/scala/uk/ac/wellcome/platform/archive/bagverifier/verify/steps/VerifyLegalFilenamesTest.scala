package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagPath,
  PayloadManifest
}
import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, MD5}

class VerifyLegalFilenamesTest
    extends AnyFunSpec
    with Matchers
    with EitherValues {
  val verifier: VerifyLegalFilenames =
    new VerifyLegalFilenames {}

  describe("verifyLegalFilenames") {
    it("allows legal filenames") {
      verifier.verifyLegalFilenames(Seq("cat.jpg", "dog.png", "fish.gif")) shouldBe Right(
        ()
      )
    }

    it("flags a file with a trailing dot") {
      val err = verifier.verifyLegalFilenames(Seq("bad.jpg.")).left.value
      err.e.getMessage shouldBe "Filenames cannot end with a .: bad.jpg."
      err.userMessage shouldBe Some("Filenames cannot end with a .: bad.jpg.")
    }

    it("flags multiple files with a trailing dot") {
      val err = verifier
        .verifyLegalFilenames(Seq("bad.jpg.", "alsobad.png.", "good.tif"))
        .left
        .value
      err.e.getMessage shouldBe "Filenames cannot end with a .: bad.jpg., alsobad.png."
      err.userMessage shouldBe Some(
        "Filenames cannot end with a .: bad.jpg., alsobad.png."
      )
    }
  }

  describe("verifyPayloadFilenames") {
    it("allows filenames that start with data/") {
      val manifest = PayloadManifest(
        checksumAlgorithm = MD5,
        entries = Map(
          BagPath("data/README.txt") -> ChecksumValue("123"),
          BagPath("data/animals/cat.jpg") -> ChecksumValue("123"),
          BagPath("data/animals/dog.png") -> ChecksumValue("123"),
        )
      )

      verifier.verifyPayloadFilenames(manifest) shouldBe Right(())
    }

    it("allows an empty manifest") {
      val manifest = PayloadManifest(
        checksumAlgorithm = MD5,
        entries = Map.empty
      )

      verifier.verifyPayloadFilenames(manifest) shouldBe Right(())
    }

    it("fails a manifest with filenames outside data/") {
      val manifest = PayloadManifest(
        checksumAlgorithm = MD5,
        entries = Map(
          BagPath("data/README.txt") -> ChecksumValue("123"),
          BagPath("data/animals/cat.jpg") -> ChecksumValue("123"),
          BagPath("dog.png") -> ChecksumValue("123"),
          BagPath("tags/metadata.csv") -> ChecksumValue("123"),
        )
      )

      val err = verifier.verifyPayloadFilenames(manifest).left.value
      err.e.getMessage shouldBe "Not all payload files are in the data/ directory: dog.png, tags/metadata.csv"
      err.userMessage shouldBe Some(
        "Not all payload files are in the data/ directory: dog.png, tags/metadata.csv"
      )
    }
  }
}
