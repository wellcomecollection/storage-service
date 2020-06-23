package uk.ac.wellcome.platform.archive.common.storage.services.s3

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.{ObjectLocation, StorageError}

class S3ObjectExistsTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with EitherValues
    with RandomThings {

  import S3ObjectExists._

  describe("S3ObjectExists") {
    describe("when an object exists in S3") {
      it("returns true") {
        withLocalS3Bucket { bucket =>
          val objectLocation = createObjectLocationWith(bucket.name)
          val inputStream = Codec.bytesCodec.toStream(randomBytes()).right.value

          putStream(location = objectLocation, inputStream = inputStream)

          objectLocation.exists.right.value shouldBe true
        }
      }
    }

    describe("when an object is not in S3") {
      it("with a valid existing bucket but no key") {
        withLocalS3Bucket { bucket =>
          val objectLocation = ObjectLocation(bucket.name, randomAlphanumeric)

          objectLocation.exists.right.value shouldBe false
        }
      }

      it("with a valid but NOT existing bucket AND no key") {
        val objectLocation =
          ObjectLocation(createBucketName, randomAlphanumeric)

        objectLocation.exists.right.value shouldBe false
      }
    }

    describe("when there is an error retrieving from S3") {
      it("with an invalid bucket name") {
        val objectLocation =
          ObjectLocation(createInvalidBucketName, randomAlphanumeric)

        objectLocation.exists.left.value shouldBe a[StorageError]
      }

      it("with a broken s3 client") {
        val objectLocation =
          ObjectLocation(createInvalidBucketName, randomAlphanumeric)

        val existsCheck = new S3ObjectExists.S3ObjectExistsImplicit(
          objectLocation
        )(brokenS3Client).exists

        existsCheck.left.value shouldBe a[StorageError]
      }
    }
  }
}
