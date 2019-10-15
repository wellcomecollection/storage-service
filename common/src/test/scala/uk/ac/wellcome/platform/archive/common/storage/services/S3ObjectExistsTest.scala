package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{ObjectLocation, StorageError}

class S3ObjectExistsTest
    extends FunSpec
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
          val byteLength = randomInt(100,200)
          val randomData = randomInputStream(byteLength)

          putStream(objectLocation, new InputStreamWithLengthAndMetadata(randomData, byteLength, Map.empty))

          objectLocation.exists.right.value shouldBe true
        }
      }
    }

    describe("when an object does not exist in S3") {
      it("returns false") {
        withLocalS3Bucket { bucket =>
          val objectLocation = ObjectLocation(bucket.name, randomAlphanumeric)

          objectLocation.exists.right.value shouldBe false
        }
      }
    }

    describe("when there is an error retrieving from S3") {
      it("returns a StorageError") {
        val objectLocation =
          ObjectLocation(randomAlphanumeric, randomAlphanumeric)

        objectLocation.exists.left.value shouldBe a[StorageError]
      }
    }
  }
}
