package uk.ac.wellcome.platform.archive.bagunpacker.services

import uk.ac.wellcome.platform.archive.bagunpacker.storage.UnarchiverError
import uk.ac.wellcome.storage.StorageError

sealed trait UnpackerError

case class UnpackerStorageError(e: StorageError) extends UnpackerError
case class UnpackerUnarchiverError(e: UnarchiverError) extends UnpackerError
