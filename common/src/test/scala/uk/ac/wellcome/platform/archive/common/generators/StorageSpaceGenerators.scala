package uk.ac.wellcome.platform.archive.common.generators

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

trait StorageSpaceGenerators extends StorageRandomThings with Logging {
  def createStorageSpace: StorageSpace = {
    val space = randomAlphanumericWithLength()
    debug(s"Creating StorageSpace: $space")
    StorageSpace(space)
  }
}
