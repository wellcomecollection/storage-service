package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.{TransferNoOp, TransferOverwriteFailure}

class AzurePutBlockFromURLTransferTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with AzureFixtures
    with AzureTransferFixtures
    with TableDrivenPropertyChecks {
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
          val srcSummary = createS3ObjectSummaryFrom(
            src,
            size = "Hello world".getBytes().length
          )

          dstStore.put(dst)("Hello world") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .right
            .value shouldBe TransferNoOp(srcSummary, dst)
        }
      }
    }

    it("identical sizes, different contents => no-op") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("hello world") shouldBe a[Right[_, _]]
          val srcSummary = createS3ObjectSummaryFrom(
            src,
            size = "hello world".getBytes().length
          )
          dstStore.put(dst)("HELLO WORLD") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .right
            .value shouldBe TransferNoOp(srcSummary, dst)
        }
      }
    }

    it("different sizes => failure") {
      withLocalS3Bucket { srcBucket =>
        withAzureContainer { dstContainer =>
          val src = createS3ObjectLocationWith(srcBucket)
          val dst = createAzureBlobLocationWith(dstContainer)

          srcStore.put(src)("Hello world") shouldBe a[Right[_, _]]
          val srcSummary = createS3ObjectSummaryFrom(
            src,
            size = "Hello world".getBytes().length
          )
          dstStore.put(dst)("Greetings, humans") shouldBe a[Right[_, _]]

          transfer
            .transfer(srcSummary, dst, checkForExisting = true)
            .left
            .value shouldBe a[TransferOverwriteFailure[_, _]]
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

          transfer.transfer(
            src = createS3ObjectSummaryFrom(src),
            dst = dst,
            checkForExisting = true
          ) shouldBe a[Right[_, _]]

          dstStore.get(dst).right.value.identifiedT shouldBe "Hello world"
        }
      }
    }
  }
}
