package weco.storage.store.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage._
import weco.storage.providers.azure.{AzureBlobLocation, AzureStorageErrors}
import weco.storage.store.RetryableReadable
import weco.storage.streaming.InputStreamWithLength

trait AzureStreamReadable
    extends RetryableReadable[AzureBlobLocation, InputStreamWithLength] {
  implicit val blobClient: BlobServiceClient

  override protected def retryableGetFunction(
    location: AzureBlobLocation): InputStreamWithLength = {
    val blobInputStream = blobClient
      .getBlobContainerClient(location.container)
      .getBlobClient(location.name)
      .openInputStream()

    new InputStreamWithLength(
      inputStream = blobInputStream,
      length = blobInputStream.getProperties.getBlobSize,
    )
  }

  override protected def buildGetError(throwable: Throwable): ReadError =
    AzureStorageErrors.readErrors(throwable)
}
