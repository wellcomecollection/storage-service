package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, PutObjectResult}
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.VerifyFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

class S3ObjectVerifierTest
    extends FunSpec
    with Matchers
    with EitherValues
    with VerifyFixture {

  it("returns a failure if the bucket doesn't exist") {
    val verifiableLocation = verifiableLocation()
    val verifiedLocation = objectVerifier.verify(verifiableLocation())

    verifiedLocation shouldBe a[VerifiedFailure]
    val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

    verifiedFailure.location shouldBe verifiableLocation
    verifiedFailure.e shouldBe a[AmazonS3Exception]
    verifiedFailure.e.getMessage should startWith(
      "The specified bucket does not exist"
    )
  }

  it("returns a failure if the object doesn't exist") {
    withLocalS3Bucket { bucket =>
      val badLocation = createObjectLocationWith(bucket)
      val verifiableLocation = verifiableLocationWith(badLocation)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedFailure]
      val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[AmazonS3Exception]
      verifiedFailure.e.getMessage should startWith(
        "The specified key does not exist"
      )
    }
  }

  it("returns a failure if the checksum is incorrect") {
    withLocalS3Bucket { bucket =>
      val objectLocation = createObjectLocationWith(bucket)
      val verifiableLocation = verifiableLocationWith(objectLocation, badChecksum)

      put(
        verifiableLocation.objectLocation,
        contents = "HelloWorld"
      )

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedFailure]
      val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[AmazonS3Exception]
      verifiedFailure.e.getMessage should startWith(
        "Checksums do not match"
      )
    }
  }

  it("returns a success if the checksum is correct") {
    withLocalS3Bucket { bucket =>
      val contentHashingAlgorithm = SHA256
      val contentString = "HelloWorld"
      // sha256("HelloWorld")
      val contentStringChecksum = ChecksumValue(
        "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
      )

      val objectLocation = createObjectLocationWith(bucket)
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      val verifiableLocation = verifiableLocationWith(objectLocation, checksum)

      put(
        verifiableLocation.objectLocation,
        contents = contentString
      )

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedSuccess]
      val verifiedSuccess = verifiedLocation.asInstanceOf[VerifiedSuccess]

      verifiedSuccess.location shouldBe verifiableLocation
    }
  }

  it("supports different checksum algorithms") {
    withLocalS3Bucket { bucket =>
      val request = VerificationRequest(
        objectLocation = createObjectLocationWith(bucket),
        checksum = Checksum(
          algorithm = "MD5",
          value = "68e109f0f40ca72a15e05cc22786f8e6" // md5("HelloWorld")
        )
      )

      put(
        request.objectLocation,
        contents = "HelloWorld"
      )

      objectVerifier.verify(request) shouldBe Right(request)
    }
  }

  def put(objectLocation: ObjectLocation,
          contents: String = randomAlphanumeric()): PutObjectResult =
    s3Client
      .putObject(objectLocation.namespace, objectLocation.key, contents)
}
