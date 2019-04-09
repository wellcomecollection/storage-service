package uk.ac.wellcome.platform.archive.bagunpacker.exceptions

import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage.ObjectLocation

class ArchiveLocationException(objectLocation: ObjectLocation, message: String)
    extends Exception(message: String) {
  def this(objectLocation: ObjectLocation,
           message: String,
           cause: AmazonS3Exception) {
    this(objectLocation, message)
    initCause(cause)
  }

  def getObjectLocation: ObjectLocation = {
    objectLocation
  }
}
