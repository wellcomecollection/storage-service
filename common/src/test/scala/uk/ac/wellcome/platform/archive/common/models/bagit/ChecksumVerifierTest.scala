package uk.ac.wellcome.platform.archive.common.models.bagit

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

class ChecksumVerifierTest extends FunSpec with Matchers with S3 {

  it("calculates the checksum") {
    withLocalS3Bucket { bucket =>
      val content = "text"
      val key = "key"
      s3Client.putObject(bucket.name, key, content)
      val contentChecksum = "982d9e3eb996f559e633f4d194def3761d909f5a3b647d1a851fead67c32c9d1"

      implicit val s3client: AmazonS3 = s3Client
      ChecksumVerifier.checksum(ObjectLocation(bucket.name, key), "sha256") shouldBe contentChecksum
    }
  }

  it("fails for an unknown algorithm") {
    implicit val s3client: AmazonS3 = s3Client
    val thrown = intercept[IllegalArgumentException] {
      ChecksumVerifier.checksum(ObjectLocation("bucket", "key"), "unknown")
    }
    thrown.getMessage shouldBe "unknown algorithm 'unknown'"
  }

  it("fails if the bucket cannot be found") {
    implicit val s3client: AmazonS3 = s3Client
    val thrown = intercept[AmazonS3Exception] {
      ChecksumVerifier.checksum(ObjectLocation("bucket", "not-there"), "sha256")
    }
    thrown.getMessage should startWith("The specified bucket does not exist.")
  }

  it("fails if the object cannot be found") {
    withLocalS3Bucket { bucket =>
      implicit val s3client: AmazonS3 = s3Client
      val thrown = intercept[AmazonS3Exception] {
        ChecksumVerifier.checksum(ObjectLocation(bucket.name, "not-there"), "sha256")
      }
      thrown.getMessage should startWith("The specified key does not exist.")
    }
  }

  //  TODO: it("fails if the object get/stream fails") {
}

