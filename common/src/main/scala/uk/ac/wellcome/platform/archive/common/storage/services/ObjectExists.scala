package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.storage.StorageError

// TODO: Push this inside the storage library
trait ObjectExists[Ident] {
  def exists(ident: Ident): Either[StorageError, Boolean]
}
