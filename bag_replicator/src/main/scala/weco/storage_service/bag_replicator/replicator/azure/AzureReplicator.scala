package weco.storage_service.bag_replicator.replicator.azure

import software.amazon.awssdk.services.s3.S3Client
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.transfer.azure.{
  AzurePrefixTransfer,
  AzureTransfer,
  SourceS3Object
}

class AzureReplicator(
  transfer: AzureTransfer[_]
)(
  implicit s3Client: S3Client
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
