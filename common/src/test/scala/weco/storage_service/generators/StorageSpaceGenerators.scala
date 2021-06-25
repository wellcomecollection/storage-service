package weco.storage_service.generators

import grizzled.slf4j.Logging
import weco.storage_service.storage.models.StorageSpace

trait StorageSpaceGenerators extends StorageRandomGenerators with Logging {
  def createStorageSpace = {
    val space = randomAlphanumeric()
    debug(s"Creating StorageSpace: $space")
    StorageSpace(space)
  }
}
