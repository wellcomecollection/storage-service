package weco.storage_service.bag_verifier.storage.azure

import java.net.URI

import com.azure.storage.blob.BlobUrlParts
import weco.storage_service.bag_verifier.storage.{
  Locatable,
  LocateFailure,
  LocationParsingError
}
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}

import scala.util.Try

class AzureLocatable
    extends Locatable[AzureBlobLocation, AzureBlobLocationPrefix, URI] {
  override def locate(uri: URI)(
    maybeRoot: Option[AzureBlobLocationPrefix]
  ): Either[LocateFailure[URI], AzureBlobLocation] =
    Try {
      val blobUrlParts = BlobUrlParts.parse(uri.toURL)

      AzureBlobLocation(
        container = blobUrlParts.getBlobContainerName,
        name = blobUrlParts.getBlobName
      )
    }.toEither.left.map { throwable =>
      LocationParsingError(
        uri,
        s"Failed parsing Azure location: ${throwable.getMessage}"
      )
    }
}
