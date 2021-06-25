package weco.storage_service.generators

import weco.storage.generators.{
  AzureBlobLocationGenerators,
  S3ObjectLocationGenerators
}
import weco.storage_service.storage.models._

trait ReplicaLocationGenerators
    extends S3ObjectLocationGenerators
    with AzureBlobLocationGenerators {
  def createPrimaryLocation: PrimaryReplicaLocation =
    chooseFrom(
      PrimaryS3ReplicaLocation(prefix = createS3ObjectLocationPrefix)
    )

  def createSecondaryLocation: SecondaryReplicaLocation =
    chooseFrom(
      SecondaryS3ReplicaLocation(prefix = createS3ObjectLocationPrefix),
      SecondaryAzureReplicaLocation(prefix = createAzureBlobLocationPrefix)
    )
}
