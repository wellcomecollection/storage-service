package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.storage.listing.s3.NewS3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.azure.{NewS3toAzurePrefixTransfer, NewS3toAzureTransfer}
import uk.ac.wellcome.storage.{AzureBlobItemLocation, AzureBlobItemLocationPrefix}

class AzureReplicator(
  implicit s3Client: AmazonS3,
  blobClient: BlobServiceClient
) extends Replicator[AzureBlobItemLocation, AzureBlobItemLocationPrefix] {

  override implicit val prefixListing: NewS3ObjectLocationListing =
    new NewS3ObjectLocationListing()

  implicit val transfer: NewS3toAzureTransfer = new NewS3toAzureTransfer()

  override implicit val prefixTransfer: NewS3toAzurePrefixTransfer =
    new NewS3toAzurePrefixTransfer()

  override protected def buildDestinationFromParts(container: String, namePrefix: String): AzureBlobItemLocationPrefix =
    AzureBlobItemLocationPrefix(container = container, namePrefix = namePrefix)
}
