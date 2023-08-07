package weco.storage.listing.azure

import java.time.Duration

import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.{BlobItem, ListBlobsOptions}
import grizzled.slf4j.Logging
import weco.storage.ListingFailure
import weco.storage.providers.azure.AzureBlobLocationPrefix

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class AzureBlobItemListing(implicit blobClient: BlobServiceClient)
    extends AzureListing[BlobItem]
    with Logging {
  override def list(prefix: AzureBlobLocationPrefix): ListingResult = {
    if (!prefix.namePrefix.endsWith("/") && prefix.namePrefix != "") {
      warn(
        "Listing an Azure prefix that does not end with a slash " +
          s"($prefix) may return unexpected blobs. " +
          "See https://alexwlchan.net/2020/08/s3-prefixes-are-not-directories/"
      )
    }

    Try {
      val containerClient = blobClient.getBlobContainerClient(prefix.container)

      val options = new ListBlobsOptions().setPrefix(prefix.namePrefix)

      val items: Iterable[BlobItem] = containerClient
        .listBlobs(options, Duration.ofSeconds(5))
        .iterator()
        .asScala
        .toIterable

      items
    } match {
      case Failure(err)   => Left(ListingFailure(prefix, err))
      case Success(items) => Right(items)
    }
  }
}
