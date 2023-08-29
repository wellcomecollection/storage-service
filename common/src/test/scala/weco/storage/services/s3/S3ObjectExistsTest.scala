package weco.storage.services.s3

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.StorageError
import weco.storage.fixtures.S3Fixtures

class S3ObjectExistsTest extends AnyFunSpec with Matchers with S3Fixtures {

  import S3ObjectExists._

  describe("S3ObjectExists") {
    describe("when an object exists in S3") {
      it("returns true") {
        withLocalS3Bucket { bucket =>
          val location = createS3ObjectLocationWith(bucket)
          putStream(location)

          location.exists.value shouldBe true
        }
      }
    }

    describe("when an object is not in S3") {
      it("with a valid existing bucket but no key") {
        withLocalS3Bucket { bucket =>
          val location = createS3ObjectLocationWith(bucket)

          location.exists.value shouldBe false
        }
      }

      it("with a valid but NOT existing bucket AND no key") {
        val location = createS3ObjectLocationWith(bucket = createBucket)

        location.exists.value shouldBe false
      }
    }

    describe("when there is an error retrieving from S3") {
      it("with an invalid bucket name") {
        val location = createS3ObjectLocationWith(bucket = createInvalidBucket)

        location.exists.left.value shouldBe a[StorageError]
      }

      it("with a broken s3 client") {
        val location = createS3ObjectLocation

        val existsCheck = new S3ObjectExists.S3ObjectExistsImplicit(location)(
          brokenS3Client
        ).exists

        existsCheck.left.value shouldBe a[StorageError]
      }
    }
  }
}
