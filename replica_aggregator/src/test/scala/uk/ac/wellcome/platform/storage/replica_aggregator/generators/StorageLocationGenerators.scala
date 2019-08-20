package uk.ac.wellcome.platform.storage.replica_aggregator.generators

import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait StorageLocationGenerators extends ObjectLocationGenerators {
  def createPrimaryLocation = PrimaryStorageLocation(
    provider = InfrequentAccessStorageProvider,
    prefix = createObjectLocationPrefix
  )

  def createSecondaryLocation = SecondaryStorageLocation(
    provider = InfrequentAccessStorageProvider,
    prefix = createObjectLocationPrefix
  )
}
