package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.BlobRange
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.storage.azure.BlobRangeUtil
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3Uploader
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.transfer.{Transfer, TransferDestinationFailure, TransferPerformed}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class AzurePutBlockTransfer(
  implicit
  s3Client: AmazonS3,
  blobServiceClient: BlobServiceClient
) extends Transfer[S3ObjectLocation, AzureBlobLocation] with Logging {

  implicit val s3Uploader: S3Uploader = new S3Uploader()

  private def singleTransfer(src: S3ObjectLocation, dst: AzureBlobLocation): TransferEither = {
    val size = s3Client.getObject(src.bucket, src.key).getObjectMetadata.getContentLength
    println(s"Size of $src is $size")

    val s3PresignedUrl = s3Uploader.getPresignedGetURL(
      location = src,
      expiryLength = 6.hour
    ).right.get

    val blockClient = blobServiceClient
      .getBlobContainerClient(dst.container)
      .getBlobClient(dst.name)
      .getBlockBlobClient

    import scala.collection.JavaConverters._

    val ranges = BlobRangeUtil.getRanges(length = size, blockSize = 100000000L)
    println(ranges)

    val blockIdLength = ranges.length.toString.length

    val identifiedRanges: Seq[(String, BlobRange)] = ranges.zipWithIndex.map { case (range, i) =>
      // https://gauravmantri.com/2013/05/18/windows-azure-blob-storage-dealing-with-the-specified-blob-or-block-content-is-invalid-error/
      val idNumber = ("0" * blockIdLength + (i + 1).toString).takeRight(blockIdLength)

      Base64.getEncoder.encodeToString(idNumber.getBytes(StandardCharsets.UTF_8)) -> range
    }
    debug(s"@@AWLC ranges for $src: $identifiedRanges")

    Try {
      identifiedRanges.foreach { case (blockId, range) =>
        debug(s"@@AWLC uploading to $dst with range $range / block Id $blockId")
        val s = blockClient.stageBlock()
        val s = blockClient.stageBlockFromUrl(
          blockId,
          s3PresignedUrl.toString,
          range
        )
        debug(s"@@AWLC reuslt of stageBlockFromURL for $blockId=$range is $s")
        println(s)
      }

      blockClient.commitBlockList(identifiedRanges.map { case (blockId, _) => blockId }.toList.asJava)
    } match {
      case Success(_)   => Right(TransferPerformed(src, dst))
      case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
    }
  }

  override protected def transferWithCheckForExisting(
    src: S3ObjectLocation,
    dst: AzureBlobLocation
  ): TransferEither = {
    import uk.ac.wellcome.storage.RetryOps._

    def inner: TransferEither =
      singleTransfer(src, dst)

    inner.retry(maxAttempts = 3)
  }

  override protected def transferWithOverwrites(src: S3ObjectLocation, dst: AzureBlobLocation): TransferEither =
    transferWithCheckForExisting(src, dst)
}
