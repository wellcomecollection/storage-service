package weco.storage.store.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage.store.StreamStore
import weco.storage.providers.azure.AzureBlobLocation

class AzureStreamStore(
  val maxRetries: Int = 2,
  val allowOverwrites: Boolean = true
)(implicit val blobClient: BlobServiceClient)
    extends StreamStore[AzureBlobLocation]
    with AzureStreamReadable
    with AzureStreamWritable
