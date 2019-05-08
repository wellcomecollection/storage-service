package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.fixtures.S3

class ObjectVerifierTest extends FunSpec with Matchers with EitherValues with S3 with RandomThings {
  val objectVerifier = new ObjectVerifier(s3Client)

  it("returns a failure if the bucket doesn't exist") {
    val result = objectVerifier.verify(
      VerificationRequest(
        objectLocation = createObjectLocation,
        checksum = Checksum(
          algorithm = "sha256",
          value = randomAlphanumeric()
        )
      )
    )

    result.isLeft shouldBe true
  }
}
