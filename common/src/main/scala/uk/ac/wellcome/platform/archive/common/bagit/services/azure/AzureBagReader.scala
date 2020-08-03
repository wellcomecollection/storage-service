package uk.ac.wellcome.platform.archive.common.bagit.services.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureBagReader(implicit blobClient: BlobServiceClient) extends BagReader[AzureBlobLocation, AzureBlobLocationPrefix]{
  override implicit val readable: store.Readable[AzureBlobLocation, InputStreamWithLength] = new AzureStreamStore()
}
