package weco.storage_service.bagit.services.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage_service.bagit.services.BagReader
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.store
import weco.storage.store.azure.AzureStreamStore
import weco.storage.streaming.InputStreamWithLength

class AzureBagReader(
  val readable: store.Readable[AzureBlobLocation, InputStreamWithLength]
) extends BagReader[AzureBlobLocation, AzureBlobLocationPrefix]

object AzureBagReader {
  def apply()(implicit blobClient: BlobServiceClient): AzureBagReader = {
    val readable = new AzureStreamStore()
    new AzureBagReader(readable)
  }
}
