package uk.ac.wellcome.platform.storage.bag_root_finder.services

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

import scala.util.Success

class S3BagLocatorTest extends AnyFunSpec with Matchers with NewS3Fixtures {
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
    keys.foreach { k =>
      s3Client.putObject(bucket.name, k, "example object")
    }
}
