package weco.storage.transfer.azure

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.{AzureFixtures, S3Fixtures}
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.azure.AzureTypedStore
import weco.storage.store.s3.S3TypedStore
import weco.storage.transfer.{TransferNoOp, TransferOverwriteFailure}

import scala.concurrent.duration._

class AzurePutBlockFromURLTransferTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with AzureFixtures
    with TableDrivenPropertyChecks {
  val srcStore: S3TypedStore[String] = S3TypedStore[String]
  val dstStore: AzureTypedStore[String] = AzureTypedStore[String]

  val transfer = AzurePutBlockFromUrlTransfer(10 minutes, 100000000L)

  describe("does a no-op transfer for similar-looking objects") {
    it("identical contents => no-op") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcObject = SourceS3Object(
            src,
            size = "Hello world".getBytes().length
          )

          dstStore.put(dst)("Hello world") shouldBe a[Right[_, _]]

          transfer.transfer(srcObject, dst).value shouldBe TransferNoOp(srcObject, dst)
        }
      }
    }

    it("identical sizes, different contents => no-op") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("hello world") shouldBe a[Right[_, _]]
          val srcObject = SourceS3Object(
            src,
            size = "hello world".getBytes().length
          )
          dstStore.put(dst)("HELLO WORLD") shouldBe a[Right[_, _]]

          transfer.transfer(srcObject, dst).value shouldBe TransferNoOp(srcObject, dst)
        }
      }
    }

    it("different sizes => failure") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcObject = SourceS3Object(
            src,
            size = "Hello world".getBytes().length
          )
          dstStore.put(dst)("Greetings, humans") shouldBe a[Right[_, _]]

          transfer.transfer(srcObject, dst).left.value shouldBe a[TransferOverwriteFailure[_, _]]
        }
      }
    }
  }

  it("handles weird keys") {
    val weirdKeys = Table(
      "key",
      "born-digital/SATSY/1821/4/v1/data/objects/Disc_4/Outreach_-_misc/PFFD_enquiry_-Dawn_G."
    )

    withLocalS3Bucket { bucket =>
      withAzureContainer { container =>
        forAll(weirdKeys) { key =>
          val src = S3ObjectLocation(bucket.name, key)
          val dst = AzureBlobLocation(container.name, key)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcObject = SourceS3Object(
            src,
            size = "Hello world".getBytes().length
          )

          transfer.transfer(src = srcObject, dst = dst) shouldBe a[Right[_, _]]

          dstStore.get(dst).value.identifiedT shouldBe "Hello world"
        }
      }
    }
  }
}
