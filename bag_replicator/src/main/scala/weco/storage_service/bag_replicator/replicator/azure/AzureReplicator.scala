package weco.storage_service.bag_replicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.transfer.azure.{
  AzurePrefixTransfer,
  AzureTransfer,
  SourceS3Object
}

class AzureReplicator(
  transfer: AzureTransfer[_]
)(
  implicit s3Client: AmazonS3
) extends Replicator[
      SourceS3Object,
      AzureBlobLocation,
      AzureBlobLocationPrefix
    ] {

  override implicit val prefixTransfer: AzurePrefixTransfer =
    new AzurePrefixTransfer()(s3Client, transfer)

  override protected def buildDestinationFromParts(
    container: String,
    namePrefix: String
  ): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(container = container, namePrefix = namePrefix)
}
