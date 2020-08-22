package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.{Transfer, TransferPerformed, TransferTestCases}

class AzurePutBlockTransferTest extends TransferTestCases[
  S3ObjectLocation,
  AzureBlobLocation,
  Record,
  Bucket, Container,
  S3TypedStore[Record], AzureTypedStore[Record],
  Unit] with S3Fixtures with AzureFixtures with StorageRandomThings {

  val blockSize: Int = 10000

  // Deliberately create quite a large record, so we can test the ability of
  // the Transfer object to join multiple blocks together.
  override def createT: Record =
    Record(name = randomAlphanumericWithLength(length = blockSize * 10))

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withDstNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createSrcLocation(bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def createDstLocation(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def withSrcStore[R](initialEntries: Map[S3ObjectLocation, Record])(
    testWith: TestWith[S3TypedStore[Record], R])(implicit context: Unit): R = {
    val store = S3TypedStore[Record]

    initialEntries.foreach { case (location, record) =>
      store.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(store)
  }

  override def withDstStore[R](initialEntries: Map[AzureBlobLocation, Record])(
    testWith: TestWith[AzureTypedStore[Record], R])(implicit context: Unit): R = {
    val store = AzureTypedStore[Record]

    initialEntries.foreach { case (location, record) =>
      store.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(store)
  }

  override def withTransfer[R](
    srcStore: S3TypedStore[Record],
    dstStore: AzureTypedStore[Record])(
    testWith: TestWith[Transfer[S3ObjectLocation, AzureBlobLocation], R]): R =
    testWith(
      new AzurePutBlockTransfer(blockSize = blockSize)
    )

  describe("it copies objects on/off the blockSize boundary") {
    it("copies an object whose size is an exact multiple of blockSize") {
      withNamespacePair { case (srcNamespace, dstNamespace) =>
        val src = createSrcLocation(srcNamespace)
        val dst = createDstLocation(dstNamespace)

        val t = Record(randomAlphanumericWithLength(length = blockSize * 10))

        withContext { implicit context =>
          withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
            withDstStore(initialEntries = Map.empty) { dstStore =>
              val result =
                withTransfer(srcStore, dstStore) {
                  _.transfer(src, dst)
                }

              result.right.value shouldBe TransferPerformed(src, dst)

              srcStore.get(src).right.value.identifiedT shouldBe t
              dstStore.get(dst).right.value.identifiedT shouldBe t
            }
          }
        }
      }
    }

    it("copies an object whose size is not an exact multiple of blockSize") {
      withNamespacePair { case (srcNamespace, dstNamespace) =>
        val src = createSrcLocation(srcNamespace)
        val dst = createDstLocation(dstNamespace)

        assert(blockSize > 1)
        val t = Record(randomAlphanumericWithLength(length = blockSize * 10 + 1))

        withContext { implicit context =>
          withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
            withDstStore(initialEntries = Map.empty) { dstStore =>
              val result =
                withTransfer(srcStore, dstStore) {
                  _.transfer(src, dst)
                }

              result.right.value shouldBe TransferPerformed(src, dst)

              srcStore.get(src).right.value.identifiedT shouldBe t
              dstStore.get(dst).right.value.identifiedT shouldBe t
            }
          }
        }
      }
    }
  }
}
