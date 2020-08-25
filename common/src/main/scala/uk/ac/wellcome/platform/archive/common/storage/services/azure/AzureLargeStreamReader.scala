package uk.ac.wellcome.platform.archive.common.storage.services.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.common.storage.services.{LargeStreamReader, RangedReader, SizeFinder}
import uk.ac.wellcome.storage.azure.AzureBlobLocation

class AzureLargeStreamReader(val bufferSize: Long)(implicit blobServiceClient: BlobServiceClient) extends LargeStreamReader[AzureBlobLocation] {
  override protected val sizeFinder: SizeFinder[AzureBlobLocation] =
    new AzureSizeFinder()

  override protected val rangedReader: RangedReader[AzureBlobLocation] =
    new AzureRangedReader()
}
