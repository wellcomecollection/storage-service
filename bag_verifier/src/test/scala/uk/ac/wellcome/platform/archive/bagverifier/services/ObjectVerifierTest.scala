package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.model.{AmazonS3Exception, PutObjectResult}
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  Checksum,
  VerificationRequest
}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.sys.process._

class ObjectVerifierTest
    extends FunSpec
    with Matchers
    with EitherValues
    with S3
    with RandomThings {
  val objectVerifier = new ObjectVerifier(s3Client)

  it("returns a failure if the bucket doesn't exist") {
    val request = VerificationRequest(
      objectLocation = createObjectLocation,
      checksum = Checksum(
        algorithm = "sha256",
        value = randomAlphanumeric()
      )
    )

    val result = objectVerifier.verify(request)

    result.isLeft shouldBe true

    val failedVerification = result.left.value
    failedVerification.request shouldBe request
    failedVerification.error shouldBe a[AmazonS3Exception]
    failedVerification.error.getMessage should startWith(
      "The specified bucket does not exist")
  }

  it("returns a failure if the object doesn't exist") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "sha256",
          value = randomAlphanumeric()
        )
      )

      val result = objectVerifier.verify(request)

      result.isLeft shouldBe true

      val failedVerification = result.left.value
      failedVerification.request shouldBe request
      failedVerification.error shouldBe a[AmazonS3Exception]
      failedVerification.error.getMessage should startWith(
        "The specified key does not exist")
    }
  }

  it("returns a failure if the algorithm doesn't exist") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "nonsense",
          value = randomAlphanumeric()
        )
      )

      put(request.objectLocation)

      val result = objectVerifier.verify(request)

      result.isLeft shouldBe true

      val failedVerification = result.left.value
      failedVerification.request shouldBe request
      failedVerification.error shouldBe a[IllegalArgumentException]
      failedVerification.error.getMessage should startWith(
        s"java.security.NoSuchAlgorithmException: ${request.checksum.algorithm} MessageDigest not available")
    }
  }

  it("returns a failure if the checksum is incorrect") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "MD5",
          value = getChecksum("HelloWorld")
        )
      )

      put(
        request.objectLocation,
        contents = "HelloWorld"
      )

      val result = objectVerifier.verify(request)

      result.isLeft shouldBe true

      val failedVerification = result.left.value
      failedVerification.request shouldBe request
      failedVerification.error shouldBe a[RuntimeException]
      failedVerification.error.getMessage should startWith(
        "Checksums do not match")
    }
  }

  it("returns a success if the checksum is correct") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "SHA-256",
          value = getChecksum("HelloWorld")
        )
      )

      put(
        request.objectLocation,
        contents = "HelloWorld"
      )

      objectVerifier.verify(request) shouldBe Right(request)
    }
  }

  it("supports different checksum algorithms") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "SHA-512",
          value = getChecksum("HelloWorld", algorithm = "sha512")
        )
      )

      put(
        request.objectLocation,
        contents = "HelloWorld"
      )

      objectVerifier.verify(request) shouldBe Right(request)
    }
  }

  def getChecksum(s: String, algorithm: String = "sha256"): String = {
    // The intention here isn't to compute a SHA256 checksum in the most
    // "Scala-like" way, it's to use an external tool that means we have an
    // independent validation of the result.
    s"echo -n $s" #| s"openssl dgst -$algorithm" !!
  }.trim

  def put(objectLocation: ObjectLocation,
          contents: String = randomAlphanumeric()): PutObjectResult =
    s3Client
      .putObject(objectLocation.namespace, objectLocation.key, contents)
}
