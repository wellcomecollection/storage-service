package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation}
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix

trait StorageLocationGenerators
    extends StorageRandomThings {
  def createProvider: StorageProvider =
    StorageProvider(
      id = chooseFrom(StorageProvider.allowedValues)
    )

  def createPrimaryLocation: PrimaryStorageLocation =
    createPrimaryLocationWith()

  def createPrimaryLocationWith(
    provider: StorageProvider = createProvider,
    prefix: S3ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    PrimaryStorageLocation(
      provider = provider,
      prefix = prefix
    )

  def createSecondaryLocation: SecondaryStorageLocation =
    createSecondaryLocationWith()

  def createSecondaryLocationWith(
    provider: StorageProvider = createProvider,
    prefix: S3ObjectLocationPrefix = createObjectLocationPrefix
  ) =
    SecondaryStorageLocation(
      provider = provider,
      prefix = prefix
    )
}
