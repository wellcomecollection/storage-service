package uk.ac.wellcome.platform.archive.bagverifier.storage

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.S3CopierFixtures
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

class S3PrefixCopierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with S3CopierFixtures {
  val s3PrefixCopier = new S3PrefixCopier(s3Client)

  it("returns a successful Future if there are no files in the prefix") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src/")
      val dst = createObjectLocationWith(bucket, key = "dst/")

      val future = s3PrefixCopier.copyObjects(src, dst)

      whenReady(future) { _ =>
        listKeysInBucket(bucket) shouldBe empty
      }
    }
  }

  it("copies a single file under a prefix") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        val src = srcPrefix.copy(key = srcPrefix.key + "foo.txt")
        createObject(src)

        val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")
        val dst = dstPrefix.copy(key = dstPrefix.key + "foo.txt")

        val future = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

        whenReady(future) { _ =>
          listKeysInBucket(dstBucket) shouldBe List("dst/foo.txt")
          assertEqualObjects(src, dst)
        }
      }
    }
  }

  it("copies multiple files under a prefix") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        val srcLocations = (1 to 5).map { i =>
          val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
          createObject(src)
          src
        }

        val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")

        val dstLocations = srcLocations.map { loc: ObjectLocation =>
          loc.copy(
            namespace = dstPrefix.namespace,
            key = loc.key.replace("src/", "dst/")
          )
        }

        val future = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

        whenReady(future) { _ =>
          listKeysInBucket(dstBucket) shouldBe dstLocations.map { _.key }

          srcLocations.zip(dstLocations).map {
            case (src, dst) =>
              assertEqualObjects(src, dst)
          }
        }
      }
    }
  }

  it("returns a failed Future if the source bucket does not exist") {
    val srcPrefix = createObjectLocation
    val dstPrefix = createObjectLocation

    val future = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

    whenReady(future.failed) { err =>
      err shouldBe a[AmazonS3Exception]
    }
  }

  it("returns a failed Future if the destination bucket does not exist") {
    withLocalS3Bucket { bucket =>
      val srcPrefix = createObjectLocationWith(bucket)

      val src = srcPrefix.copy(key = srcPrefix.key + "/foo.txt")
      createObject(src)

      val dstPrefix = createObjectLocationWith(Bucket("no_such_bucket"))

      val future = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

      whenReady(future.failed) { err =>
        err shouldBe a[AmazonS3Exception]
      }
    }
  }

  ignore("copies more objects than are returned in a single ListObject call") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        // You can get up to 1000 objects in a single S3 ListObject call.
        val srcLocations = (1 to 1100).map { i =>
          val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
          createObject(src)
          src
        }

        val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")

        val dstLocations = srcLocations.map { loc: ObjectLocation =>
          loc.copy(
            namespace = dstPrefix.namespace,
            key = loc.key.replace("src/", "dst/")
          )
        }

        val future = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

        whenReady(future) { _ =>
          val actualKeys = listKeysInBucket(dstBucket)
          val expectedKeys = dstLocations.map { _.key }
          actualKeys.size shouldBe expectedKeys.size
          actualKeys should contain theSameElementsAs expectedKeys

          srcLocations.zip(dstLocations).map {
            case (src, dst) =>
              assertEqualObjects(src, dst)
          }
        }
      }
    }
  }
}
