package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.model.S3ObjectSummary
import com.azure.storage.blob.models.BlobRange
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.{StreamStore, TypedStore}
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength}
import uk.ac.wellcome.storage.transfer.{
  Transfer,
  TransferDestinationFailure,
  TransferPerformed,
  TransferTestCases
}

class AzurePutBlockTransferTest
    extends TransferTestCases[
      S3ObjectSummary,
      AzureBlobLocation,
      Record,
      Bucket,
      Container,
      TypedStore[S3ObjectSummary, Record],
      AzureTypedStore[Record],
      Unit
    ]
    with S3Fixtures
    with AzureFixtures
    with StorageRandomThings {

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

  override def createSrcLocation(bucket: Bucket): S3ObjectSummary = {
    val src = createS3ObjectLocationWith(bucket)
    val summary = new S3ObjectSummary()
    summary.setBucketName(src.bucket)
    summary.setKey(src.key)

    // By default, the size of an S3ObjectSummary() is zero.  We don't want it to
    // be zero, because then the underlying S3 SDK will skip trying to read it;
    // the correct size will be set in the StreamStore[S3ObjectSummary].
    summary.setSize(randomInt(from = 1, to = 50))

    summary
  }

  override def createDstLocation(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  val summaryStreamStore: StreamStore[S3ObjectSummary] =
    new StreamStore[S3ObjectSummary] {
      private val underlying = new S3StreamStore()

      override def get(summary: S3ObjectSummary): ReadEither =
        underlying
          .get(S3ObjectLocation(summary))
          .map { case Identified(_, result) => Identified(summary, result) }

      override def put(
        summary: S3ObjectSummary
      )(is: InputStreamWithLength): WriteEither =
        underlying
          .put(S3ObjectLocation(summary))(is)
          .map {
            case Identified(_, result) =>
              summary.setSize(is.length)
              Identified(summary, result)
          }
    }

  val stringStore: TypedStore[S3ObjectSummary, String] =
    new TypedStore[S3ObjectSummary, String] {
      override implicit val codec: Codec[String] = Codec.stringCodec

      override implicit val streamStore: StreamStore[S3ObjectSummary] =
        summaryStreamStore
    }

  override def withSrcStore[R](
    initialEntries: Map[S3ObjectSummary, Record]
  )(
    testWith: TestWith[TypedStore[S3ObjectSummary, Record], R]
  )(implicit context: Unit): R = {
    val c = implicitly[Codec[Record]]

    val typedStore: TypedStore[S3ObjectSummary, Record] =
      new TypedStore[S3ObjectSummary, Record] {
        override implicit val codec: Codec[Record] = c

        override implicit val streamStore: StreamStore[S3ObjectSummary] =
          summaryStreamStore
      }

    initialEntries.foreach {
      case (summary, record) =>
        typedStore.put(summary)(record) shouldBe a[Right[_, _]]
    }

    testWith(typedStore)
  }

  override def withDstStore[R](initialEntries: Map[AzureBlobLocation, Record])(
    testWith: TestWith[AzureTypedStore[Record], R]
  )(implicit context: Unit): R = {
    val store = AzureTypedStore[Record]

    initialEntries.foreach {
      case (location, record) =>
        store.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(store)
  }

  override def withTransfer[R](
    srcStore: TypedStore[S3ObjectSummary, Record],
    dstStore: AzureTypedStore[Record]
  )(testWith: TestWith[Transfer[S3ObjectSummary, AzureBlobLocation], R]): R =
    testWith(
      new AzurePutBlockTransfer(blockSize = blockSize)
    )

  describe("it copies objects on/off the blockSize boundary") {
    it("copies an object whose size is an exact multiple of blockSize") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
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
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val src = createSrcLocation(srcNamespace)
          val dst = createDstLocation(dstNamespace)

          assert(blockSize > 1)
          val t =
            Record(randomAlphanumericWithLength(length = blockSize * 10 + 1))

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

  it("skips blocks that have already been written") {
    withNamespacePair {
      case (srcBucket, dstContainer) =>
        val src = createSrcLocation(srcBucket)
        val dst = createDstLocation(dstContainer)

        stringStore.put(src)("Hello world") shouldBe a[Right[_, _]]

        // Write the first block to the destination blob
        val blockClient = azureClient
          .getBlobContainerClient(dst.container)
          .getBlobClient(dst.name)
          .getBlockBlobClient

        val blockId = BlobRangeUtil.getBlockIdentifiers(count = 3).head

        blockClient.stageBlock(
          blockId,
          new ByteArrayInputStream("Hello".getBytes()),
          5
        )

        var howManyBlocksWritten = 0

        val transfer = new AzurePutBlockTransfer(blockSize = 5L) {
          override def writeBlockToAzure(
            src: S3ObjectLocation,
            dst: AzureBlobLocation,
            range: BlobRange,
            blockId: String,
            s3Length: Long,
            context: Unit
          ): Unit = {
            howManyBlocksWritten += 1
            super.writeBlockToAzure(src, dst, range, blockId, s3Length, context)
          }
        }

        val result = transfer.transfer(src, dst)
        result.right.value shouldBe TransferPerformed(src, dst)

        // "Hello world" is 11 bytes long.  Bytes 1-5 were already written, which means
        // the transfer should have written 6-10 and 11-.
        howManyBlocksWritten shouldBe 2
    }
  }

  describe("retrying a failed Put Block operation") {
    class AzureFlakyBlockTransfer(maxFailures: Int)
        extends AzurePutBlockTransfer(blockSize = 5L) {
      var failures = 0

      override def writeBlockToAzure(
        src: S3ObjectLocation,
        dst: AzureBlobLocation,
        range: BlobRange,
        blockId: String,
        s3Length: Long,
        context: Unit
      ): Unit = {
        failures += 1
        if (failures > maxFailures) {
          super.writeBlockToAzure(src, dst, range, blockId, s3Length, context)
        } else {
          throw new Throwable("BOOM! This is a flaky failure")
        }
      }
    }

    it("retries a single flaky error") {
      withNamespacePair {
        case (srcBucket, dstContainer) =>
          val src = createSrcLocation(srcBucket)
          val dst = createDstLocation(dstContainer)

          stringStore.put(src)("Hello world") shouldBe a[Right[_, _]]

          val transfer = new AzureFlakyBlockTransfer(maxFailures = 1)

          val result = transfer.transfer(src, dst)
          result.right.value shouldBe TransferPerformed(src, dst)

          AzureTypedStore[String]
            .get(dst)
            .right
            .value
            .identifiedT shouldBe "Hello world"
      }
    }

    it("does not retry errors forever") {
      withNamespacePair {
        case (srcBucket, dstContainer) =>
          val src = createSrcLocation(srcBucket)
          val dst = createDstLocation(dstContainer)

          S3TypedStore[String].put(
            S3ObjectLocation(src)
          )("Hello world") shouldBe a[Right[_, _]]

          val transfer = new AzureFlakyBlockTransfer(maxFailures = Int.MaxValue)

          val result = transfer.transfer(src, dst)
          result.left.value shouldBe a[TransferDestinationFailure[_, _]]
      }
    }
  }
}
