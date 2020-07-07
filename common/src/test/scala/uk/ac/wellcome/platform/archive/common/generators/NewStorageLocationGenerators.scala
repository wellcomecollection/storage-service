package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures

trait NewStorageLocationGenerators
  extends NewS3Fixtures
    with StorageRandomThings {
  def createNewPrimaryLocation: PrimaryNewStorageLocation =
    createNewPrimaryLocationWith()

  def createNewPrimaryLocationWith(
    prefix: S3ObjectLocationPrefix = createS3ObjectLocationPrefix
  ): PrimaryNewStorageLocation =
    PrimaryS3StorageLocation(prefix = prefix)

  def createNewSecondaryLocation: SecondaryNewStorageLocation =
    createNewSecondaryLocationWith()

  def createNewSecondaryLocationWith(
    prefix: S3ObjectLocationPrefix = createS3ObjectLocationPrefix
  ): SecondaryNewStorageLocation =
    SecondaryS3StorageLocation(prefix = prefix)
}
