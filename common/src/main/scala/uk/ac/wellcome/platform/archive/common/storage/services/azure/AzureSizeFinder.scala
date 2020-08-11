package uk.ac.wellcome.platform.archive.common.storage.services.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureStorageErrors}
import uk.ac.wellcome.storage.store.RetryableReadable

class AzureSizeFinder(val maxRetries: Int = 3)(implicit blobServiceClient: BlobServiceClient) extends SizeFinder[AzureBlobLocation] with RetryableReadable[AzureBlobLocation, Long] {
  override def retryableGetFunction(location: AzureBlobLocation): Long =
    blobServiceClient
      .getBlobContainerClient(location.container)
      .getBlobClient(location.name)
      .getProperties
      .getBlobSize

  override def buildGetError(throwable: Throwable): ReadError = AzureStorageErrors.readErrors(throwable)
}
