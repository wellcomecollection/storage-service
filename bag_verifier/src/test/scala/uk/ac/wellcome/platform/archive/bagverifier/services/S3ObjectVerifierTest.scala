package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.VerifyFixtures
import uk.ac.wellcome.platform.archive.common.storage.{LocationError, LocationNotFound}
import uk.ac.wellcome.platform.archive.common.verify._

class S3ObjectVerifierTest
    extends FunSpec
    with Matchers
    with EitherValues
    with VerifyFixtures {

  it("returns a failure if the bucket doesn't exist") {
    val badVerifiableLocation = createVerifiableLocation
    val verifiedLocation = objectVerifier.verify(badVerifiableLocation)

    verifiedLocation shouldBe a[VerifiedFailure]
    val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

    verifiedFailure.location shouldBe badVerifiableLocation
    verifiedFailure.e shouldBe a[LocationError[_]]
    verifiedFailure.e.getMessage should include(
      "The specified bucket is not valid"
    )
  }

  it("returns a failure if the object doesn't exist") {
    withLocalS3Bucket { bucket =>
      val badLocation = createObjectLocationWith(bucket)
      val verifiableLocation = createVerifiableLocationWith(location = badLocation)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedFailure]
      val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[LocationNotFound[_]]
      verifiedFailure.e.getMessage should include(
        "Location not available!"
      )
    }
  }

  it("returns a failure if the checksum is incorrect") {
    withLocalS3Bucket { bucket =>
      val objectLocation = createObjectLocationWith(bucket)
      val verifiableLocation = createVerifiableLocationWith(
        location = objectLocation,
        checksum = badChecksum
      )

      createObject(objectLocation, content = "HelloWorld")

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedFailure]
      val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[FailedChecksumNoMatch]
      verifiedFailure.e.getMessage should startWith(
        "Checksum values do not match"
      )
    }
  }

  it("fails if the checksum is correct but the expected length is wrong") {
    withLocalS3Bucket { bucket =>
      val contentHashingAlgorithm = MD5
      val contentString = "HelloWorld"
      // md5("HelloWorld")
      val contentStringChecksum = ChecksumValue(
        "68e109f0f40ca72a15e05cc22786f8e6"
      )

      val objectLocation = createObjectLocationWith(bucket)
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      val verifiableLocation = createVerifiableLocationWith(
        location = objectLocation,
        checksum = checksum,
        length = Some(contentString.getBytes().length - 1)
      )

      createObject(objectLocation, content = contentString)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedFailure]
      val verifiedFailure = verifiedLocation.asInstanceOf[VerifiedFailure]

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[LocationNotFound[_]]
      verifiedFailure.e.getMessage should include(
        "Lengths do not match!"
      )
    }
  }

  it("returns a success if the checksum is correct") {
    withLocalS3Bucket { bucket =>
      val contentHashingAlgorithm = MD5
      val contentString = "HelloWorld"
      // md5("HelloWorld")
      val contentStringChecksum = ChecksumValue(
        "68e109f0f40ca72a15e05cc22786f8e6"
      )

      val objectLocation = createObjectLocationWith(bucket)
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      val verifiableLocation = createVerifiableLocationWith(
        location = objectLocation,
        checksum = checksum
      )

      createObject(objectLocation, content = contentString)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedSuccess]
      val verifiedSuccess = verifiedLocation.asInstanceOf[VerifiedSuccess]

      verifiedSuccess.location shouldBe verifiableLocation
    }
  }

  it("succeeds if the checksum is correct and the lengths match") {
    withLocalS3Bucket { bucket =>
      val contentHashingAlgorithm = MD5
      val contentString = "HelloWorld"
      // md5("HelloWorld")
      val contentStringChecksum = ChecksumValue(
        "68e109f0f40ca72a15e05cc22786f8e6"
      )

      val objectLocation = createObjectLocationWith(bucket)
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      val verifiableLocation = createVerifiableLocationWith(
        location = objectLocation,
        checksum = checksum,
        length = Some(contentString.getBytes().length)
      )

      createObject(objectLocation, content = contentString)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedSuccess]
      val verifiedSuccess = verifiedLocation.asInstanceOf[VerifiedSuccess]

      verifiedSuccess.location shouldBe verifiableLocation
    }
  }

  it("supports different checksum algorithms") {
    withLocalS3Bucket { bucket =>
      val contentHashingAlgorithm = SHA256
      val contentString = "HelloWorld"
      // sha256("HelloWorld")
      val contentStringChecksum = ChecksumValue(
        "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
      )

      val objectLocation = createObjectLocationWith(bucket)
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      val verifiableLocation = createVerifiableLocationWith(
        location = objectLocation,
        checksum = checksum
      )

      createObject(objectLocation, content = contentString)

      val verifiedLocation = objectVerifier.verify(verifiableLocation)

      verifiedLocation shouldBe a[VerifiedSuccess]
      val verifiedSuccess = verifiedLocation.asInstanceOf[VerifiedSuccess]

      verifiedSuccess.location shouldBe verifiableLocation
    }
  }
}
