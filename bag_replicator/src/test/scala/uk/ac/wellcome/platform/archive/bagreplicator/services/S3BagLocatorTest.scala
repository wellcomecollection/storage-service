package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Success

class S3BagLocatorTest extends FunSpec with Matchers with S3 {
  it("locates a bag-info.txt file in the root") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(bucket,
        "bag123/bag-info.txt", "bag123/data/1.jpg", "bag123/data/2.jpg"
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")

      s3BagLocator.locateBagInfo(objectLocation) shouldBe Success(
        createObjectLocationWith(bucket, "bag123/bag-info.txt")
      )
    }
  }

  it("finds a bag-info.txt in an immediate subdirectory") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(bucket,
        "bag123/subdir/bag-info.txt",
        "bag123/subdir/data/1.jpg",
        "bag123/subdir/data/2.jpg"
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")

      s3BagLocator.locateBagInfo(objectLocation) shouldBe Success(
        createObjectLocationWith(bucket, "bag123/subdir/bag-info.txt")
      )
    }
  }

  it("prefers the bag-info.txt in the root to a subdirectory") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(bucket,
        "bag123/bag-info.txt",
        "bag123/subdir/bag-info.txt",
        "bag123/subdir/data/1.jpg",
        "bag123/subdir/data/2.jpg"
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")

      s3BagLocator.locateBagInfo(objectLocation) shouldBe Success(
        createObjectLocationWith(bucket, "bag123/bag-info.txt")
      )
    }
  }

  it("fails if there are multiple subdirectories") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(bucket,
        "bag123/subdir1/bag-info.txt",
        "bag123/subdir2/bag-info.txt",
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")
      assertFailsToFindBagIn(objectLocation)
    }
  }

  it("fails if the bag-info.txt is nested more than one directory deep") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(bucket,
        "bag123/subdir/nesteddir/bag-info.txt",
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")
      assertFailsToFindBagIn(objectLocation)
    }
  }

  it("fails if it cannot find a bag-info.txt") {
    withLocalS3Bucket { bucket =>
      val objectLocation = createObjectLocationWith(bucket, "doesnotexist")
      assertFailsToFindBagIn(objectLocation)
    }
  }

  val s3BagLocator = new S3BagLocator(s3Client)

  private def assertFailsToFindBagIn(objectLocation: ObjectLocation): Assertion = {
    val result = s3BagLocator.locateBagInfo(objectLocation)
    result.isFailure shouldBe true
    result.failed.get shouldBe a[IllegalArgumentException]
  }

  def createObjectsWith(bucket: Bucket, keys: String*): Unit =
    keys.foreach { k =>
      s3Client.putObject(bucket.name, k, "example object")
    }
}
