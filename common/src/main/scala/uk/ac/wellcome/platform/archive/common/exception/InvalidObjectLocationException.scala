package uk.ac.wellcome.platform.archive.common.exception

import uk.ac.wellcome.storage.ObjectLocation

class InvalidObjectLocationException(objectLocation: ObjectLocation, message: String) extends Exception(message: String) {
  def this(objectLocation: ObjectLocation, message: String, cause: Throwable) {
    this(objectLocation, message)
    initCause(cause)
  }

  def getObjectLocation: ObjectLocation = {
    objectLocation
  }
}