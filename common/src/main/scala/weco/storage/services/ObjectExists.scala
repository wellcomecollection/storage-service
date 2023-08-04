package weco.storage.services

import weco.storage.StorageError

trait ObjectExists[Ident] {
  def exists(id: Ident): Either[StorageError, Boolean]
}
