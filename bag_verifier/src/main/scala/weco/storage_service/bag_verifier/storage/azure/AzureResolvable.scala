package weco.storage_service.bag_verifier.storage.azure

import java.net.URI

import com.azure.storage.blob.BlobServiceClient
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage.providers.azure.AzureBlobLocation

class AzureResolvable(implicit blobServiceClient: BlobServiceClient)
    extends Resolvable[AzureBlobLocation] {
  override def resolve(location: AzureBlobLocation): URI = {
    new URI(
      blobServiceClient
        .getBlobContainerClient(location.container)
        .getBlobClient(location.name)
        .getBlobUrl
    )
  }
}
