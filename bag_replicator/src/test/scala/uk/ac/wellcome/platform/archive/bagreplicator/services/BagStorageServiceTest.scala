package uk.ac.wellcome.platform.archive.bagreplicator.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.storage.{S3Copier, S3PrefixCopier}
import uk.ac.wellcome.platform.archive.common.models.bagit.ExternalIdentifier
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class BagStorageServiceTest extends FunSpec with Matchers with ScalaFutures with BagReplicatorFixtures with S3 {

  val s3PrefixCopier = new S3PrefixCopier(
    s3Client = s3Client,
    copier = new S3Copier(s3Client)
  )
  val bagStorage = new BagStorageService(s3PrefixCopier = s3PrefixCopier)

  it("duplicates a bag within the same bucket") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { srcBagLocation =>
        val destinationConfig = ReplicatorDestinationConfig(
          namespace = bucket.name,
          rootPath = randomAlphanumeric()
        )

        val future = bagStorage.duplicateBag(
          sourceBagLocation = srcBagLocation,
          destinationConfig = destinationConfig
        )

        whenReady(future) { result =>
          val dstBagLocation = result.right.get
          verifyBagCopied(
            src = srcBagLocation,
            dst = dstBagLocation
          )
        }
      }
    }
  }

  it("duplicates a bag across different buckets") {
    withLocalS3Bucket { sourceBucket =>
      withLocalS3Bucket { destinationBucket =>
        withBag(sourceBucket) { srcBagLocation =>
          val destinationConfig = ReplicatorDestinationConfig(
            namespace = destinationBucket.name,
            rootPath = randomAlphanumeric()
          )

          val future = bagStorage.duplicateBag(
            sourceBagLocation = srcBagLocation,
            destinationConfig = destinationConfig
          )

          whenReady(future) { result =>
            val dstBagLocation = result.right.get
            verifyBagCopied(
              src = srcBagLocation,
              dst = dstBagLocation
            )
          }
        }
      }
    }
  }

  describe("when other bags have the same prefix") {
    it("should duplicate a bag into a given location") {
      val bagInfo1 = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("prefix")
      )

      val bagInfo2 = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("prefix_suffix")
      )

      withLocalS3Bucket { sourceBucket =>
        withLocalS3Bucket { destinationBucket =>
          withBag(sourceBucket, bagInfo = bagInfo1) { srcBagLocation =>
            withBag(sourceBucket, bagInfo = bagInfo2) { _ =>
              val destinationConfig = ReplicatorDestinationConfig(
                namespace = destinationBucket.name,
                rootPath = randomAlphanumeric()
              )

              val future = bagStorage.duplicateBag(
                sourceBagLocation = srcBagLocation,
                destinationConfig = destinationConfig
              )

              whenReady(future) { result =>
                val dstBagLocation = result.right.get
                verifyBagCopied(
                  src = srcBagLocation,
                  dst = dstBagLocation
                )
              }
            }
          }
        }
      }
    }
  }

  it("returns a Left[Throwable] if there's an error while copying the bag") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { srcBagLocation =>
        val destinationConfig = ReplicatorDestinationConfig(
          namespace = "does-not-exist",
          rootPath = randomAlphanumeric()
        )

        val future = bagStorage.duplicateBag(
          sourceBagLocation = srcBagLocation,
          destinationConfig = destinationConfig
        )

        whenReady(future) { result =>
          val exception = result.left.get
          exception shouldBe a[AmazonS3Exception]
        }
      }
    }
  }
}
