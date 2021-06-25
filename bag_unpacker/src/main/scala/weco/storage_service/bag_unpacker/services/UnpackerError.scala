package weco.storage_service.bag_unpacker.services

import weco.storage_service.bag_unpacker.storage.UnarchiverError
import weco.storage.StorageError

sealed trait UnpackerError {
  val e: Throwable
}

case class UnpackerStorageError(storageError: StorageError)
    extends UnpackerError {
  override val e: Throwable = storageError.e
}

case class UnpackerUnarchiverError(unarchiverError: UnarchiverError)
    extends UnpackerError {
  override val e: Throwable = unarchiverError.e
}

case class UnpackerEOFError(e: Throwable) extends UnpackerError

case class UnpackerUnexpectedError(e: Throwable) extends UnpackerError
