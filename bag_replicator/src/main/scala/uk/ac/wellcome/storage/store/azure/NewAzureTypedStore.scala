package uk.ac.wellcome.storage.store.azure

import uk.ac.wellcome.storage.AzureBlobItemLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.streaming.Codec

class NewAzureTypedStore[T](
  implicit val codec: Codec[T],
  val streamStore: NewAzureStreamStore
) extends TypedStore[AzureBlobItemLocation, T]
