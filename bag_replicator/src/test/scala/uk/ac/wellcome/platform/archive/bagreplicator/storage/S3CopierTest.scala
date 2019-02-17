package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.S3CopierFixtures
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

class S3CopierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with S3CopierFixtures {

  val s3Copier = new S3Copier(s3Client)

  it("copies a file inside a bucket") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src.txt")
      val dst = createObjectLocationWith(bucket, key = "dst.txt")

      createObject(src)
      listKeysInBucket(bucket) shouldBe List(src.key)

      val future = s3Copier.copy(src = src, dst = dst)

      whenReady(future) { _ =>
        listKeysInBucket(bucket) should contain theSameElementsAs List(
          src.key,
          dst.key)
        assertEqualObjects(src, dst)
      }
    }
  }

  it("copies a file across different buckets") {
    withLocalS3Bucket { srcBucket =>
      val src = createObjectLocationWith(srcBucket)

      withLocalS3Bucket { dstBucket =>
        val dst = createObjectLocationWith(dstBucket)

        createObject(src)
        listKeysInBucket(srcBucket) shouldBe List(src.key)
        listKeysInBucket(dstBucket) shouldBe List()

        val future = s3Copier.copy(src = src, dst = dst)

        whenReady(future) { _ =>
          listKeysInBucket(srcBucket) shouldBe List(src.key)
          listKeysInBucket(dstBucket) shouldBe List(dst.key)
          assertEqualObjects(src, dst)
        }
      }
    }
  }

  it("returns a failed Future if the source object does not exist") {
    val src = createObjectLocation
    val dst = createObjectLocation

    val future = s3Copier.copy(src, dst)

    whenReady(future.failed) { err =>
      err shouldBe a[AmazonS3Exception]
    }
  }

  it("returns a failed Future if the destination bucket does not exist") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket)
      val dst = createObjectLocationWith(Bucket("no_such_bucket"))

      val future = s3Copier.copy(src, dst)

      whenReady(future.failed) { err =>
        err shouldBe a[AmazonS3Exception]
      }
    }
  }
}
