package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.StorageSpace

trait StorageSpaceGenerators extends RandomThings {
  def createStorageSpace = StorageSpace(randomAlphanumeric())
}
