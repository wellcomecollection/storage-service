package weco.storage.services.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage.ReadError
import weco.storage.azure.{AzureBlobLocation, AzureStorageErrors}
import weco.storage.services.SizeFinder
import weco.storage.store.RetryableReadable

class AzureSizeFinder(val maxRetries: Int = 3)(
  implicit blobServiceClient: BlobServiceClient
) extends SizeFinder[AzureBlobLocation]
    with RetryableReadable[AzureBlobLocation, Long] {
  override def retryableGetFunction(location: AzureBlobLocation): Long =
    blobServiceClient
      .getBlobContainerClient(location.container)
      .getBlobClient(location.name)
      .getProperties
      .getBlobSize

  override def buildGetError(throwable: Throwable): ReadError =
    AzureStorageErrors.readErrors(throwable)
}
