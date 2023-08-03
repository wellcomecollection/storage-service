package weco.storage.transfer.azure

import java.io.ByteArrayInputStream

import com.azure.storage.blob.models.BlobRange
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.storage.Identified
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.fixtures.{AzureFixtures, S3Fixtures}
import weco.storage.generators.Record
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.azure.AzureTypedStore
import weco.storage.store.s3.{S3StreamStore, S3TypedStore}
import weco.storage.store.{StreamStore, TypedStore}
import weco.storage.streaming.{Codec, InputStreamWithLength}
import weco.storage.transfer.{
  Transfer,
  TransferDestinationFailure,
  TransferPerformed,
  TransferTestCases
}

class AzurePutBlockTransferTest
    extends TransferTestCases[
      SourceS3Object,
      AzureBlobLocation,
      Record,
      Bucket,
      Container,
      TypedStore[SourceS3Object, Record],
      AzureTypedStore[Record],
      Unit
    ]
    with S3Fixtures
    with AzureFixtures {

  val blockSize: Int = 10000

  // Deliberately create quite a large record, so we can test the ability of
  // the Transfer object to join multiple blocks together.
  override def createT: Record =
    Record(name = randomAlphanumeric(length = blockSize * 10))

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withDstNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createSrcLocation(bucket: Bucket): SourceS3Object = {
    val src = createS3ObjectLocationWith(bucket)
    SourceS3Object(src, size = blockSize * 10)
  }

  override def createDstLocation(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  private def getSizeOf(r: Record): Long = {
    val c = implicitly[Codec[Record]]
    c.toStream(r).value.length
  }

  override def createSrcObject(bucket: Bucket): (SourceS3Object, Record) = {
    val record = createT
    val location = createS3ObjectLocationWith(bucket)
    val src = SourceS3Object(location, size = getSizeOf(record))

    (src, record)
  }

  val summaryStreamStore: StreamStore[SourceS3Object] =
    new StreamStore[SourceS3Object] {
      private val underlying = new S3StreamStore()

      override def get(summary: SourceS3Object): ReadEither =
        underlying
          .get(summary.location)
          .map { case Identified(_, result) => Identified(summary, result) }

      override def put(
        summary: SourceS3Object
      )(is: InputStreamWithLength): WriteEither =
        underlying
          .put(summary.location)(is)
          .map {
            case Identified(_, result) =>
              Identified(summary.copy(size = is.length), result)
          }
    }

  val stringStore: TypedStore[SourceS3Object, String] =
    new TypedStore[SourceS3Object, String] {
      override implicit val codec: Codec[String] = Codec.stringCodec

      override implicit val streamStore: StreamStore[SourceS3Object] =
        summaryStreamStore
    }

  override def withSrcStore[R](
    initialEntries: Map[SourceS3Object, Record]
  )(
    testWith: TestWith[TypedStore[SourceS3Object, Record], R]
  )(implicit context: Unit): R = {
    val c = implicitly[Codec[Record]]

    val typedStore: TypedStore[SourceS3Object, Record] =
      new TypedStore[SourceS3Object, Record] {
        override implicit val codec: Codec[Record] = c

        override implicit val streamStore: StreamStore[SourceS3Object] =
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
    srcStore: TypedStore[SourceS3Object, Record],
    dstStore: AzureTypedStore[Record]
  )(testWith: TestWith[Transfer[SourceS3Object, AzureBlobLocation], R]): R =
    testWith(
      new AzurePutBlockTransfer(blockSize = blockSize)
    )

  describe("it copies objects on/off the blockSize boundary") {
    it("copies an object whose size is an exact multiple of blockSize") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val dst = createDstLocation(dstNamespace)

          val t = Record(randomAlphanumeric(length = blockSize * 10))

          val src = SourceS3Object(
            location = createS3ObjectLocationWith(bucket = srcNamespace),
            size = getSizeOf(t)
          )

          withContext { implicit context =>
            withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
              withDstStore(initialEntries = Map.empty) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.value shouldBe TransferPerformed(src, dst)

                srcStore.get(src).value.identifiedT shouldBe t
                dstStore.get(dst).value.identifiedT shouldBe t
              }
            }
          }
      }
    }

    it("copies an object whose size is not an exact multiple of blockSize") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val dst = createDstLocation(dstNamespace)

          assert(blockSize > 1)
          val t = Record(randomAlphanumeric(length = blockSize * 10 + 1))

          val src = SourceS3Object(
            location = createS3ObjectLocationWith(bucket = srcNamespace),
            size = getSizeOf(t)
          )

          withContext { implicit context =>
            withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
              withDstStore(initialEntries = Map.empty) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.value shouldBe TransferPerformed(src, dst)

                srcStore.get(src).value.identifiedT shouldBe t
                dstStore.get(dst).value.identifiedT shouldBe t
              }
            }
          }
      }
    }
  }

  it("skips blocks that have already been written") {
    withNamespacePair {
      case (srcBucket, dstContainer) =>
        val src = SourceS3Object(
          location = createS3ObjectLocationWith(bucket = srcBucket),
          size = 11
        )
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
        result.value shouldBe TransferPerformed(src, dst)

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
          val src = SourceS3Object(
            location = createS3ObjectLocationWith(bucket = srcBucket), size = 11
          )
          val dst = createDstLocation(dstContainer)

          stringStore.put(src)("Hello world") shouldBe a[Right[_, _]]

          val transfer = new AzureFlakyBlockTransfer(maxFailures = 1)

          val result = transfer.transfer(src, dst)
          result.value shouldBe TransferPerformed(src, dst)

          AzureTypedStore[String]
            .get(dst)
            .value
            .identifiedT shouldBe "Hello world"
      }
    }

    it("does not retry errors forever") {
      withNamespacePair {
        case (srcBucket, dstContainer) =>
          val src = createSrcLocation(srcBucket)
          val dst = createDstLocation(dstContainer)

          S3TypedStore[String].put(src.location)("Hello world") shouldBe a[Right[_, _]]

          val transfer = new AzureFlakyBlockTransfer(maxFailures = Int.MaxValue)

          val result = transfer.transfer(src, dst)
          result.left.value shouldBe a[TransferDestinationFailure[_, _]]
      }
    }
  }
}
