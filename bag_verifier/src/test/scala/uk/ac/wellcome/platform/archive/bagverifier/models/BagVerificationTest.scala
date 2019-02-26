package uk.ac.wellcome.platform.archive.bagverifier.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagItemPath}

class BagVerificationTest extends FunSpec with Matchers with RandomThings {
  it("reports a verification with no failures as successful") {
    val result = BagVerification(
      woke = Seq(createBagDigestFile, createBagDigestFile, createBagDigestFile),
      problematicFaves = List.empty
    )

    result.verificationSucceeded shouldBe true
  }

  it("reports a verification with some problems as unsuccessful") {
    val result = BagVerification(
      woke = Seq(createBagDigestFile, createBagDigestFile),
      problematicFaves = List(
        FailedVerification(
          digestFile = createBagDigestFile,
          reason = new RuntimeException("AAARGH!")
        )
      )
    )

    result.verificationSucceeded shouldBe false
  }

  def createBagDigestFile: BagDigestFile =
    BagDigestFile(
      checksum = randomAlphanumeric(),
      path = BagItemPath(randomAlphanumeric())
    )
}
