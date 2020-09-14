package uk.ac.wellcome.platform.archive.common.bagit.services.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureBagReader(val readable: store.Readable[AzureBlobLocation, InputStreamWithLength])
    extends BagReader[AzureBlobLocation, AzureBlobLocationPrefix]

object AzureBagReader {
  def apply()(implicit blobClient: BlobServiceClient): AzureBagReader = {
    val readable= new AzureStreamStore()
    new AzureBagReader(readable)
  }
}
