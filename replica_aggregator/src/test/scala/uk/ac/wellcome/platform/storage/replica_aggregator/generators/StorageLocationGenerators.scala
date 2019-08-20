package uk.ac.wellcome.platform.storage.replica_aggregator.generators

import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  SecondaryStorageLocation
}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait StorageLocationGenerators extends ObjectLocationGenerators {
  def createPrimaryLocation: PrimaryStorageLocation =
    createPrimaryLocationWith()

  def createPrimaryLocationWith(
    prefix: ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    PrimaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      prefix = prefix
    )

  def createSecondaryLocation: SecondaryStorageLocation =
    createSecondaryLocationWith()

  def createSecondaryLocationWith(
    prefix: ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    SecondaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      prefix = prefix
    )
}
