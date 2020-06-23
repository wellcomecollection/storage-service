package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.storage.{ObjectLocation, StorageError}

trait ObjectExists {
  def exists(objectLocation: ObjectLocation): Either[StorageError, Boolean]
}
