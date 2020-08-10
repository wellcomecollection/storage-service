package uk.ac.wellcome.platform.archive.common.storage.services.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureStorageErrors}
import uk.ac.wellcome.storage.store.RetryableReadable

class AzureSizeFinder(val maxRetries: Int = 3)(implicit azureClient: BlobServiceClient) extends SizeFinder[AzureBlobLocation] with RetryableReadable[AzureBlobLocation, Long] {
  override def retryableGetFunction(id: AzureBlobLocation): Long = azureClient.getBlobContainerClient(id.container).getBlobClient(id.name).getProperties.getBlobSize

  override def buildGetError(throwable: Throwable): ReadError = AzureStorageErrors.readErrors(throwable)
}
