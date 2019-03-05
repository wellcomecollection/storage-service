package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.{Duration, Instant}

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
      failedVerifications = List.empty
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
      )
    )

    result.verificationSucceeded shouldBe false
  }

  it("calculates duration once completed") {
    val result = BagVerification(
      startTime = Instant.now.minus(Duration.ofSeconds(1))
    )
    val completed = result.complete
    completed.duration shouldBe defined
    completed.duration.get.toMillis shouldBe >(0l)
  }

  it("calculates duration as None when verification is not completed") {
    val result = BagVerification(
      startTime = Instant.now
    )
    result.duration shouldBe None
  }

  def createBagDigestFile: BagDigestFile =
    BagDigestFile(
      checksum = randomAlphanumeric(),
      path = BagItemPath(randomAlphanumeric())
    )
}
