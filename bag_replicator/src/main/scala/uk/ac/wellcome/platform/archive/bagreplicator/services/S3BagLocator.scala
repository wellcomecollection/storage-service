package uk.ac.wellcome.platform.archive.bagreplicator.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class S3BagLocator(s3Client: AmazonS3) {
  def locateBagInfo(objectLocation: ObjectLocation): Try[ObjectLocation] =
    findBagInfoInRoot(objectLocation) match {
      case Some(key) => Success(objectLocation.copy(key = key))
      case None => Failure(new IllegalArgumentException("Unable to locate root of bag"))
    }

  private def findBagInfoInRoot(objectLocation: ObjectLocation): Option[String] = {
    val listObjectsResult = s3Client.listObjectsV2(
      objectLocation.namespace,
      createBagInfoPath(objectLocation.key)
    )

    val keyCount = listObjectsResult.getObjectSummaries.size()

    if (keyCount == 1) {
      Some(createBagInfoPath(objectLocation.key))
    } else {
      None
    }
  }

  private def createBagInfoPath(prefix: String): String =
    prefix.stripSuffix("/") + "/bag-info.txt"
}