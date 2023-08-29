package weco.storage.store.azure

import com.azure.storage.blob.BlobServiceClient
import weco.storage.providers.azure.AzureBlobLocation
import weco.storage.store.TypedStore
import weco.storage.streaming.Codec

class AzureTypedStore[T](
  implicit val codec: Codec[T],
  val streamStore: AzureStreamStore
) extends TypedStore[AzureBlobLocation, T]

object AzureTypedStore {
  def apply[T](implicit codec: Codec[T],
               blobServiceClient: BlobServiceClient): AzureTypedStore[T] = {
    implicit val streamStore: AzureStreamStore = new AzureStreamStore()

    new AzureTypedStore[T]()
  }
}
