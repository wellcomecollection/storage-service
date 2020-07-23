package uk.ac.wellcome.storage.store.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.storage.{AzureBlobItemLocation, Identified}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class NewAzureStreamStore(
  val maxRetries: Int = 2,
  val allowOverwrites: Boolean = true
)(implicit val blobClient: BlobServiceClient)
    extends StreamStore[AzureBlobItemLocation] {

  private val underlying = new AzureStreamStore()

  override def get(location: AzureBlobItemLocation): ReadEither =
    underlying
      .get(location.toObjectLocation)
      .map {
        case Identified(_, inputStream) => Identified(location, inputStream)
      }

  override def put(
    location: AzureBlobItemLocation
  )(inputStream: InputStreamWithLength): WriteEither =
    underlying
      .put(location.toObjectLocation)(inputStream)
      .map { case Identified(_, t) => Identified(location, t) }
}
