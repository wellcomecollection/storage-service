package uk.ac.wellcome.platform.archive.bagverifier.storage.azure

import java.net.URI

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.storage.azure.AzureBlobLocation

class AzureResolvable(implicit blobServiceClient: BlobServiceClient) extends Resolvable[AzureBlobLocation] {
  override def resolve(location: AzureBlobLocation): URI = {
    new URI(blobServiceClient.getBlobContainerClient(location.container).getBlobClient(location.name).getBlobUrl)
  }
}
