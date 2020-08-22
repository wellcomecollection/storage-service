package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.BlobRange
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.s3.S3RangedReader
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.transfer._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait AzureTransfer[Context] extends Transfer[S3ObjectLocation, AzureBlobLocation] with Logging {
  implicit val s3Client: AmazonS3
  implicit val blobServiceClient: BlobServiceClient

  val blockSize: Int

  private val s3SizeFinder = new S3SizeFinder()

  def runTransfer(src: S3ObjectLocation, dst: AzureBlobLocation): Either[TransferFailure[S3ObjectLocation, AzureBlobLocation], Unit] =
    for {
      s3Length <- s3SizeFinder.getSize(src) match {
        case Left(readError) => Left(TransferSourceFailure(src, dst, readError.e))
        case Right(result)   => Right(result)
      }

      context <- getContext(src)

      result <- writeBlocks(src = src, dst = dst, s3Length = s3Length, context = context) match {
        case Success(_)   => Right(())
        case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
      }
    } yield result

  protected def getContext(src: S3ObjectLocation): Either[TransferSourceFailure[S3ObjectLocation, AzureBlobLocation], Context]

  protected def writeBlockToAzure(
    src: S3ObjectLocation,
    dst: AzureBlobLocation,
    range: BlobRange,
    blockId: String,
    s3Length: Long,
    context: Context
  ): Unit

  protected def writeBlocks(
    src: S3ObjectLocation,
    dst: AzureBlobLocation,
    s3Length: Long,
    context: Context
  ): Try[Unit] = {
    val blockClient = blobServiceClient
      .getBlobContainerClient(dst.container)
      .getBlobClient(dst.name)
      .getBlockBlobClient

    val ranges = BlobRangeUtil.getRanges(length = s3Length, blockSize = blockSize)
    val identifiers = BlobRangeUtil.getBlockIdentifiers(count = ranges.size)

    Try {
      identifiers.zip(ranges).foreach { case (blockId, range) =>
        debug(s"Uploading to $dst with range $range / block Id $blockId")
        writeBlockToAzure(
          src = src,
          dst = dst,
          range = range,
          blockId = blockId,
          s3Length = s3Length,
          context = context
        )
      }

      blockClient.commitBlockList(identifiers.toList.asJava)
    }
  }

  override protected def transferWithCheckForExisting(src: S3ObjectLocation, dst: AzureBlobLocation): TransferEither =
    runTransfer(src, dst).map { _ => TransferPerformed(src, dst) }

  override protected def transferWithOverwrites(src: S3ObjectLocation, dst: AzureBlobLocation): TransferEither =
    runTransfer(src, dst).map { _ => TransferPerformed(src, dst) }
}

class AzurePutBlockTransfer(
  // The max length you can put in a single Put Block API call is 4000 MiB.
  // The class will load a block of this size into memory, so setting it too
  // high may cause issues.
  val blockSize: Int = 1000000000
)(
  implicit
  val s3Client: AmazonS3,
  val blobServiceClient: BlobServiceClient
) extends AzureTransfer[Unit] {

  private val rangedReader = new S3RangedReader()

  override protected def getContext(src: S3ObjectLocation): Either[TransferSourceFailure[S3ObjectLocation, AzureBlobLocation], Unit] =
    Right(())

  override protected def writeBlockToAzure(
    src: S3ObjectLocation,
    dst: AzureBlobLocation,
    range: BlobRange,
    blockId: String,
    s3Length: Long,
    context: Unit
  ): Unit = {
    val bytes = rangedReader.getBytes(
      location = src,
      offset = range.getOffset,
      count = range.getCount,
      totalLength = s3Length
    )

    val blockClient = blobServiceClient
      .getBlobContainerClient(dst.container)
      .getBlobClient(dst.name)
      .getBlockBlobClient

    blockClient.upload(new ByteArrayInputStream(bytes), bytes.size)
  }
}
