package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.{Duration, Instant}

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagDigestFile,
  BagItemPath
}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.generators.BagLocationGenerators
import uk.ac.wellcome.storage.fixtures.S3

class VerificationSummaryTest
    extends FunSpec
    with Matchers
    with RandomThings
    with BagLocationGenerators
    with S3 {
  it("reports a verification with no failures as successful") {
    val result = VerificationSummary(
      createObjectLocation,
      successfulVerifications =
        Seq(createBagDigestFile, createBagDigestFile, createBagDigestFile),
      failedVerifications = List.empty
    )

    result.succeeded shouldBe true
  }

  it("reports a verification with some problems as unsuccessful") {
    val result = VerificationSummary(
      createObjectLocation,
      successfulVerifications = Seq(createBagDigestFile, createBagDigestFile),
      failedVerifications = List(
        FailedVerification(
          digestFile = createBagDigestFile,
          reason = new RuntimeException("AAARGH!")
        )
      )
    )

    result.succeeded shouldBe false
  }

  it("calculates duration once completed") {
    val result = VerificationSummary(
      createObjectLocation,
      startTime = Instant.now.minus(Duration.ofSeconds(1))
    )
    val completed = result.complete
    completed.duration shouldBe defined
    completed.duration.get.toMillis shouldBe >(0l)
  }

  it("calculates duration as None when verification is not completed") {
    val result = VerificationSummary(
      createObjectLocation,
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
