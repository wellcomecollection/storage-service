package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.azure.S3toAzurePrefixTransfer

class AzureReplicator(
  implicit s3Client: AmazonS3,
  blobClient: BlobServiceClient
) extends Replicator[AzureBlobLocation, AzureBlobLocationPrefix] {

  override implicit val prefixListing: S3ObjectLocationListing =
    S3ObjectLocationListing()

  override implicit val prefixTransfer: S3toAzurePrefixTransfer =
    S3toAzurePrefixTransfer()

  override protected def buildDestinationFromParts(
    container: String,
    namePrefix: String
  ): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(container = container, namePrefix = namePrefix)
}
