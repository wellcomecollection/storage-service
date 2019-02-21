package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.S3CopierFixtures
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

class S3PrefixCopierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with S3CopierFixtures {

  val copier = new S3Copier(s3Client)
  val s3PrefixCopier = new S3PrefixCopier(
    s3Client = s3Client,
    copier = copier
  )

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

  it("copies more objects than are returned in a single ListObject call") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        // You can get up to 1000 objects in a single S3 ListObject call.
        val count = 1001
        val srcLocations = (1 to count).par.map { i =>
          val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
          createObject(src)
          src
        }

        // Check the listKeys call can really retrieve all the objects
        // we're about to copy around!
        listKeysInBucket(srcBucket) should have size count

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

  it("returns a failed Future if one of the objects fails to copy") {
    val exception = new RuntimeException("Nope, that's not going to work")

    val brokenCopier = new ObjectCopier {
      def copy(src: ObjectLocation, dst: ObjectLocation): Unit =
        if (src.key.endsWith("5.txt"))
          throw exception
    }

    val brokenPrefixCopier = new S3PrefixCopier(s3Client, copier = brokenCopier)

    withLocalS3Bucket { srcBucket =>
      val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

      (1 to 10).par.map { i =>
        val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
        createObject(src)
        src
      }

      val future = brokenPrefixCopier.copyObjects(
        srcLocationPrefix = srcPrefix,
        dstLocationPrefix = srcPrefix.copy(key = "dst/")
      )

      whenReady(future.failed) { err =>
        err shouldBe exception
      }
    }
  }

  // A modified version of listKeysInBucket that can retrieve everything,
  // even if it takes multiple ListObject calls.
  override def listKeysInBucket(bucket: Bucket): List[String] =
    S3Objects
      .inBucket(s3Client, bucket.name)
      .withBatchSize(1000)
      .iterator()
      .asScala
      .toList
      .par
      .map { objectSummary: S3ObjectSummary =>
        objectSummary.getKey
      }
      .toList

}
