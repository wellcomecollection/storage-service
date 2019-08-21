package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StandardStorageProvider,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  SecondaryStorageLocation
}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait StorageLocationGenerators extends ObjectLocationGenerators with StorageRandomThings {
  def createProvider: StorageProvider =
    chooseFrom(
      Seq(
        StandardStorageProvider,
        InfrequentAccessStorageProvider
      )
    )

  def createPrimaryLocation: PrimaryStorageLocation =
    createPrimaryLocationWith()

  def createPrimaryLocationWith(
    provider: StorageProvider = createProvider,
    prefix: ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    PrimaryStorageLocation(
      provider = provider,
      prefix = prefix
    )

  def createSecondaryLocation: SecondaryStorageLocation =
    createSecondaryLocationWith()

  def createSecondaryLocationWith(
    provider: StorageProvider = createProvider,
    prefix: ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    SecondaryStorageLocation(
      provider = provider,
      prefix = prefix
    )
}
