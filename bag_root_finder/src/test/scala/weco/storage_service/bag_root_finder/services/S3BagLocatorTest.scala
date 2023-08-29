package weco.storage_service.bag_root_finder.services

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.util.Success

class S3BagLocatorTest extends AnyFunSpec with Matchers with S3Fixtures {
  describe("locateBagInfo") {
    it("locates a bag-info.txt file in the root") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/bag-info.txt",
          "bag123/data/1.jpg",
          "bag123/data/2.jpg"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")

        s3BagLocator.locateBagInfo(prefix) shouldBe Success(
          createS3ObjectLocationWith(bucket, key = "bag123/bag-info.txt")
        )
      }
    }

    it("finds a bag-info.txt in an immediate subdirectory") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/subdir/bag-info.txt",
          "bag123/subdir/data/1.jpg",
          "bag123/subdir/data/2.jpg"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")

        s3BagLocator.locateBagInfo(prefix) shouldBe Success(
          createS3ObjectLocationWith(bucket, key = "bag123/subdir/bag-info.txt")
        )
      }
    }

    it("prefers the bag-info.txt in the root to a subdirectory") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/bag-info.txt",
          "bag123/subdir/bag-info.txt",
          "bag123/subdir/data/1.jpg",
          "bag123/subdir/data/2.jpg"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")

        s3BagLocator.locateBagInfo(prefix) shouldBe Success(
          createS3ObjectLocationWith(bucket, key = "bag123/bag-info.txt")
        )
      }
    }

    it("fails if there are multiple subdirectories") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/subdir1/bag-info.txt",
          "bag123/subdir2/bag-info.txt"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")
        assertFailsToFindBagIn(prefix)
      }
    }

    it("fails if there's a single subdirectory but without a bag-info.txt") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(bucket, "bag123/subdir/1.jpg", "bag123/subdir/2.jpg")

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")
        assertFailsToFindBagIn(prefix)
      }
    }

    it("fails if the bag-info.txt is nested more than one directory deep") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(bucket, "bag123/subdir/nesteddir/bag-info.txt")

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")
        assertFailsToFindBagIn(prefix)
      }
    }

    it("fails if it cannot find a bag-info.txt") {
      withLocalS3Bucket { bucket =>
        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "doesnotexist")
        assertFailsToFindBagIn(prefix)
      }
    }

    it("fails if the bucket does not exist") {
      val prefix = createS3ObjectLocationPrefixWith(bucket = createBucket)
      assertFailsToFindBagIn(prefix)
    }
  }

  describe("locateBagRoot") {
    it("locates a bag-info.txt file in the root") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/bag-info.txt",
          "bag123/data/1.jpg",
          "bag123/data/2.jpg"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")

        s3BagLocator.locateBagRoot(prefix) shouldBe Success(prefix)
      }
    }

    it("finds a bag-info.txt in an immediate subdirectory") {
      withLocalS3Bucket { bucket =>
        createObjectsWith(
          bucket,
          "bag123/subdir/bag-info.txt",
          "bag123/subdir/data/1.jpg",
          "bag123/subdir/data/2.jpg"
        )

        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123")

        s3BagLocator.locateBagRoot(prefix) shouldBe Success(
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "bag123/subdir")
        )
      }
    }

    it("fails if it cannot find a bag-info.txt") {
      withLocalS3Bucket { bucket =>
        val prefix =
          createS3ObjectLocationPrefixWith(bucket, keyPrefix = "doesnotexist")

        val result = s3BagLocator.locateBagRoot(prefix)
        result.isFailure shouldBe true
        result.failed.get shouldBe a[IllegalArgumentException]
      }
    }
  }

  val s3BagLocator = new S3BagLocator(s3Client)

  private def assertFailsToFindBagIn(
    prefix: S3ObjectLocationPrefix
  ): Assertion = {
    val result = s3BagLocator.locateBagInfo(prefix)
    result.isFailure shouldBe true
    result.failed.get shouldBe a[IllegalArgumentException]
  }

  def createObjectsWith(bucket: Bucket, keys: String*): Unit =
    keys.foreach { key =>
      putString(S3ObjectLocation(bucket.name, key), contents = "example object")
    }

  // TODO: Upstream into scala-libs
  def createS3ObjectLocationPrefixWith(
    bucket: Bucket,
    keyPrefix: String
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(bucket = bucket.name, keyPrefix = keyPrefix)

  def createS3ObjectLocationWith(
    bucket: Bucket,
    key: String
  ): S3ObjectLocation =
    S3ObjectLocation(bucket = bucket.name, key = key)
}
