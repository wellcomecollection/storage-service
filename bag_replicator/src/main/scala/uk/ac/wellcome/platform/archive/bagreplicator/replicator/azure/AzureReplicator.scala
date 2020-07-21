package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.storage.listing.s3.{NewS3ObjectLocationListing, S3ObjectLocationListing}
import uk.ac.wellcome.storage.store.s3.{S3StreamReadable, S3StreamStore}
import uk.ac.wellcome.storage.transfer.PrefixTransfer
import uk.ac.wellcome.storage.transfer.azure.{
  S3toAzurePrefixTransfer,
  S3toAzureTransfer
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

class AzureReplicator(
  implicit s3Client: AmazonS3,
  blobClient: BlobServiceClient
) extends Replicator {

  implicit val readable: S3StreamReadable = new S3StreamStore()

  override implicit val prefixListing: NewS3ObjectLocationListing =
    new NewS3ObjectLocationListing()

  implicit val listing: S3ObjectLocationListing =
    S3ObjectLocationListing()

  implicit val transfer: S3toAzureTransfer = new S3toAzureTransfer

  override implicit val prefixTransfer
    : PrefixTransfer[ObjectLocationPrefix, ObjectLocation] =
    new S3toAzurePrefixTransfer()
}
