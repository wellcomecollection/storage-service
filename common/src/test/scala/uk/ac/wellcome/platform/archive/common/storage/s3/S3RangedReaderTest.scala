package uk.ac.wellcome.platform.archive.common.storage.s3

import com.azure.storage.blob.models.BlobRange
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class S3RangedReaderTest extends AnyFunSpec with Matchers with S3Fixtures {
  val rangedReader = new S3RangedReader()

  it("reads part of an object in S3") {
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      s3Client.putObject(location.bucket, location.key, "Hello world")

      rangedReader.getBytes(location, offset = 0, count = 5, totalLength = 11) shouldBe "Hello"
        .getBytes()
    }
  }

  it("reads to the end of an object in S3") {
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      s3Client.putObject(location.bucket, location.key, "Hello world")

      rangedReader.getBytes(location, offset = 6, count = 5, totalLength = 11) shouldBe "world"
        .getBytes()
    }
  }

  it("if count is null, it reads to the end") {
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      s3Client.putObject(location.bucket, location.key, "Hello world")

      val range = new BlobRange(5)
      assert(range.getCount == null)

      rangedReader.getBytes(
        location,
        offset = 6,
        count = range.getCount,
        totalLength = 11
      ) shouldBe "world".getBytes()
    }
  }

  it("rejects an offset > length") {
    val location = createS3ObjectLocation

    val error = intercept[IllegalArgumentException] {
      rangedReader.getBytes(location, offset = 10, count = 5, totalLength = 5)
    }

    error.getMessage should startWith("Offset is after the end of the object")
  }
}
