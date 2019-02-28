package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Duration

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagItemPath
}

class BagVerificationTest extends FunSpec with Matchers with RandomThings {
  it("reports a verification with no failures as successful") {
    val result = BagVerification(
      successfulVerifications =
        Seq(createBagDigestFile, createBagDigestFile, createBagDigestFile),
      failedVerifications = List.empty,
      duration = Duration.ofSeconds(1)
    )

    result.verificationSucceeded shouldBe true
  }

  it("reports a verification with some problems as unsuccessful") {
    val result = BagVerification(
      successfulVerifications = Seq(createBagDigestFile, createBagDigestFile),
      failedVerifications = List(
        FailedVerification(
          digestFile = createBagDigestFile,
          reason = new RuntimeException("AAARGH!")
        )
      ),
      duration = Duration.ofSeconds(1)
    )

    result.verificationSucceeded shouldBe false
  }

  def createBagDigestFile: BagDigestFile =
    BagDigestFile(
      checksum = randomAlphanumeric(),
      path = BagItemPath(randomAlphanumeric())
    )
}
