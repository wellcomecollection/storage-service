package uk.ac.wellcome.platform.archive.bagunpacker.exceptions

import uk.ac.wellcome.storage.ObjectLocation

class UnpackerArchiveEntryUploadException(dstLocation: ObjectLocation,
                                          message: String)
    extends Exception(message: String) {
  def this(objectLocation: ObjectLocation, message: String, cause: Throwable) {
    this(objectLocation, message)
    initCause(cause)
  }

  def getObjectLocation: ObjectLocation = {
    dstLocation
  }
}
