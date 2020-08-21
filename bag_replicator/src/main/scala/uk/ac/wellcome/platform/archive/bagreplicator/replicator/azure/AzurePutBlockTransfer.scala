package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import java.net.URL

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3Uploader
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.transfer.{Transfer, TransferDestinationFailure, TransferPerformed, TransferSourceFailure}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class AzurePutBlockTransfer(
  implicit
  s3Client: AmazonS3,
  blobServiceClient: BlobServiceClient
) extends Transfer[S3ObjectLocation, AzureBlobLocation] {

  implicit val s3Uploader: S3Uploader = new S3Uploader()

  private def singleTransfer(src: S3ObjectLocation, dst: AzureBlobLocation): TransferEither =
    for {
      s3PresignedUrl: URL <- s3Uploader.getPresignedGetURL(
        location = src,
        expiryLength = 6.hour
      ) match {
        case Right(url) => Right(url)
        case Left(err)  => Left(TransferSourceFailure(src, dst, err.e))
      }

      blobClient = blobServiceClient
        .getBlobContainerClient(dst.container)
        .getBlobClient(dst.name)

      result <- Try { blobClient.copyFromUrl(s3PresignedUrl.toString) } match {
        case Success(_)   => Right(TransferPerformed(src, dst))
        case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
      }
    } yield result

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
