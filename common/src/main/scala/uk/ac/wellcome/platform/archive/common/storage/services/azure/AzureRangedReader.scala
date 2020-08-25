package uk.ac.wellcome.platform.archive.common.storage.services.azure

import java.io.ByteArrayOutputStream

import com.azure.core.util.Context
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.{BlobRange, BlobRequestConditions, DownloadRetryOptions}
import uk.ac.wellcome.platform.archive.common.storage.models.{ByteRange, ClosedByteRange, OpenByteRange}
import uk.ac.wellcome.platform.archive.common.storage.services.RangedReader
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureStorageErrors}

import scala.util.{Failure, Success, Try}

class AzureRangedReader(implicit blobServiceClient: BlobServiceClient) extends RangedReader[AzureBlobLocation] {
  override def getBytes(location: AzureBlobLocation, range: ByteRange): Either[ReadError, Array[Byte]] = {
    val blobClient =
      blobServiceClient
        .getBlobContainerClient(location.container)
        .getBlobClient(location.name)

    val stream = new ByteArrayOutputStream()

    val blobRange = range match {
      case ClosedByteRange(start, offset) => new BlobRange(start, offset)
      case OpenByteRange(start) => new BlobRange(start)
    }

    Try {
      blobClient.downloadWithResponse(
        stream, blobRange, new DownloadRetryOptions, new BlobRequestConditions, false, null, Context.NONE
      )

      stream.toByteArray
    } match {
      case Success(bytes) => Right(bytes)
      case Failure(err)   => Left(AzureStorageErrors.readErrors(err))
    }
  }
}
