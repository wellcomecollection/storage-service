package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.BlobRange
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.transfer.{Transfer, TransferDestinationFailure, TransferFailure, TransferSourceFailure}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait AzureTransfer[Context] extends Transfer[S3ObjectLocation, AzureBlobLocation] with Logging {
  implicit val s3Client: AmazonS3
  implicit val blobServiceClient: BlobServiceClient

  val blockSize: Int

  private val s3SizeFinder = new S3SizeFinder()

  def runTransfer(src: S3ObjectLocation, dst: AzureBlobLocation): Either[TransferFailure[S3ObjectLocation, AzureBlobLocation], Unit] =
    for {
      s3Size <- s3SizeFinder.getSize(src) match {
        case Left(readError) => Left(TransferSourceFailure(src, dst, readError.e))
        case Right(result)   => Right(result)
      }

      context <- getContext(src)

      result <- writeBlocks(src = src, dst = dst, s3Size = s3Size, context = context) match {
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
    context: Context
  ): Unit

  protected def writeBlocks(
    src: S3ObjectLocation,
    dst: AzureBlobLocation,
    s3Size: Long,
    context: Context
  ): Try[Unit] = {
    val blockClient = blobServiceClient
      .getBlobContainerClient(dst.container)
      .getBlobClient(dst.name)
      .getBlockBlobClient

    val ranges = BlobRangeUtil.getRanges(length = s3Size, blockSize = blockSize)
    val identifiers = BlobRangeUtil.getBlockIdentifiers(count = ranges.size)

    Try {
      identifiers.zip(ranges).foreach { case (blockId, range) =>
        debug(s"Uploading to $dst with range $range / block Id $blockId")
        writeBlockToAzure(
          src = src,
          dst = dst,
          range = range,
          blockId = blockId,
          context = context
        )
      }

      blockClient.commitBlockList(identifiers.toList.asJava)
    }
  }
}