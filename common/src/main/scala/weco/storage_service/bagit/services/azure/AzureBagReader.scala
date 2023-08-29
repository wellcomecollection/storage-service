package weco.storage_service.bagit.services.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.store.Readable
import weco.storage.store.azure.AzureStreamStore
import weco.storage.streaming.InputStreamWithLength
import weco.storage_service.bagit.services.BagReader

class AzureBagReader()(implicit blobClient: BlobServiceClient)
    extends BagReader[AzureBlobLocation, AzureBlobLocationPrefix] {
  override implicit val readable
    : Readable[AzureBlobLocation, InputStreamWithLength] =
    new AzureStreamStore()
}
