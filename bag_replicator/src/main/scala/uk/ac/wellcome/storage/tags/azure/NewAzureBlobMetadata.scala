package uk.ac.wellcome.storage.tags.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.storage.{AzureBlobItemLocation, Identified, StoreWriteError, WriteError}
import uk.ac.wellcome.storage.tags.Tags

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class NewAzureBlobMetadata(val maxRetries: Int = 2)(
  implicit blobClient: BlobServiceClient)
  extends Tags[AzureBlobItemLocation] {

  private val underlying = new AzureBlobMetadata()

  override protected def writeTags(
    location: AzureBlobItemLocation,
    tags: Map[String, String]
  ): Either[WriteError, Map[String, String]] =
    Try {
      val individualBlobClient =
        blobClient
          .getBlobContainerClient(location.container)
          .getBlobClient(location.name)

      individualBlobClient.setMetadata(tags.asJava)
    } match {
      case Success(_)   => Right(tags)
      case Failure(err) => Left(StoreWriteError(err))
    }

  override def get(location: AzureBlobItemLocation): ReadEither =
    underlying.get(location.toObjectLocation)
      .map { case Identified(_, t) => Identified(location, t) }
}