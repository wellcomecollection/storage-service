package uk.ac.wellcome.platform.archive.common.bagit.services

import com.amazonaws.services.s3.model.PutObjectResult
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Success

class S3BagFileTest extends FunSpec with Matchers with RandomThings with S3 {
  val s3BagFile = new S3BagFile(s3Client = s3Client, batchSize = 10)

  it("detects a bag-info.txt in an S3 bucket") {
    withLocalS3Bucket { bucket =>
      putObject(bucket)
      putObject(bucket)
      putObject(bucket, key = "/foo/bag-info.txt")

      s3BagFile.locateBagInfo(
        objectLocation = ObjectLocation(
          namespace = bucket.name,
          key = "/"
        )
      ) shouldBe Success("/foo/bag-info.txt")

      s3BagFile.locateBagInfo(
        objectLocation = ObjectLocation(
          namespace = bucket.name,
          key = "/foo"
        )
      ) shouldBe Success("/foo/bag-info.txt")
    }
  }

  it("ignores a bag-info.txt in a different prefix") {
    withLocalS3Bucket { bucket =>
      putObject(bucket)
      putObject(bucket)
      putObject(bucket, key = "/foo/bag-info.txt")
      putObject(bucket, key = "/bar/bag-info.txt")

      s3BagFile.locateBagInfo(
        objectLocation = ObjectLocation(
          namespace = bucket.name,
          key = "/bar"
        )
      ) shouldBe Success("/bar/bag-info.txt")
    }
  }

  it("errors if it cannot find a bag-info.txt") {
    withLocalS3Bucket { bucket =>
      putObject(bucket)
      putObject(bucket)
      putObject(bucket, key = "/foo/bag-info.txt")

      s3BagFile
        .locateBagInfo(
          objectLocation = ObjectLocation(
            namespace = bucket.name,
            key = "/bar"
          )
        )
        .isFailure shouldBe true
    }
  }

  it("looks through the entire bag, even if it's bigger than the batch size") {
    withLocalS3Bucket { bucket =>
      (1 to 20).foreach { _ =>
        putObject(bucket)
      }
      putObject(bucket, key = "/foo/bag-info.txt")

      s3BagFile.locateBagInfo(
        objectLocation = ObjectLocation(
          namespace = bucket.name,
          key = "/"
        )
      ) shouldBe Success("/foo/bag-info.txt")
    }
  }

  private def putObject(bucket: Bucket,
                        key: String = randomAlphanumeric()): PutObjectResult =
    s3Client.putObject(bucket.name, key, randomAlphanumeric())
}
