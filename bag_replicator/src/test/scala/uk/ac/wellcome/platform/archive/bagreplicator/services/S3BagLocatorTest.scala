package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Success

class S3BagLocatorTest extends FunSpec with Matchers with S3 {
  it("locates a bag-info.txt file in the root") {
    withLocalS3Bucket { bucket =>
      createObjectWith(bucket,
        "/bag123/bag-info.txt", "/bag123/data/1.jpg", "/bag123/data/2.jpg"
      )

      val objectLocation = createObjectLocationWith(bucket, "/bag123")

      s3BagLocator.locateBagInfo(objectLocation) shouldBe Success(
        createObjectLocationWith(bucket, "/bag123/bag-info.txt")
      )
    }
  }

  val s3BagLocator = new S3BagLocator(s3Client)

  def createObjectWith(bucket: Bucket, keys: String*): Unit =
    keys.foreach { k =>
      s3Client.putObject(bucket.name, k, "example object")
    }
}
