package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class VerifyLegalFilenamesTest extends AnyFunSpec with Matchers with EitherValues {
  val verifier: VerifyLegalFilenames =
    new VerifyLegalFilenames {}

  it("allows legal filenames") {
    verifier.verifyLegalFilenames(Seq("cat.jpg", "dog.png", "fish.gif")) shouldBe Right(())
  }

  it("flags a file with a trailing dot") {
    val err = verifier.verifyLegalFilenames(Seq("bad.jpg.")).left.value
    err.e.getMessage shouldBe "Filenames cannot end with a .: bad.jpg."
    err.userMessage shouldBe Some("Filenames cannot end with a .: bad.jpg.")
  }

  it("flags multiple files with a trailing dot") {
    val err = verifier.verifyLegalFilenames(Seq("bad.jpg.", "alsobad.png.", "good.tif")).left.value
    err.e.getMessage shouldBe "Filenames cannot end with a .: bad.jpg., alsobad.png."
    err.userMessage shouldBe Some("Filenames cannot end with a .: bad.jpg., alsobad.png.")
  }
}
