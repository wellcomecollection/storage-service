package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import com.amazonaws.services.s3.iterable.S3Objects
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.{TransferNoOp, TransferOverwriteFailure}
import scala.collection.convert.ImplicitConversions._

class AzurePutBlockFromURLTransferTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with AzureFixtures {
  val srcStore: S3TypedStore[String] = S3TypedStore[String]
  val dstStore: AzureTypedStore[String] = AzureTypedStore[String]

  val transfer = new AzurePutBlockFromUrlTransfer()

  describe("does a no-op transfer for similar-looking objects") {
    it("identical contents => no-op") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcSummary = S3Objects.withPrefix(s3Client, src.bucket, src.key).head

          dstStore.put(dst)("Hello world") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .right
            .value shouldBe TransferNoOp(src, dst)
        }
      }
    }

    it("identical sizes, different contents => no-op") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("hello world") shouldBe a[Right[_, _]]
          val srcSummary = S3Objects.withPrefix(s3Client, src.bucket, src.key).head
          dstStore.put(dst)("HELLO WORLD") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .right
            .value shouldBe TransferNoOp(src, dst)
        }
      }
    }

    it("different sizes => failure") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcSummary = S3Objects.withPrefix(s3Client, src.bucket, src.key).head
          dstStore.put(dst)("Greetings, humans") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .left
            .value shouldBe a[TransferOverwriteFailure[_, _]]
        }
      }
    }
  }
}
