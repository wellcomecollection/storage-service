package uk.ac.wellcome.platform.archive.bagreplicator.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3BagLocator(s3Client: AmazonS3) extends Logging {
  def locateBagInfo(objectLocation: ObjectLocation): Try[ObjectLocation] = {
    val bagInfoInRoot = findBagInfoInRoot(objectLocation)
    val bagInfoInDirectory = findBagInfoInDirectory(objectLocation)

    (bagInfoInRoot, bagInfoInDirectory) match {
      case (Success(key), _) => Success(objectLocation.copy(key = key))
      case (_, Success(key)) => Success(objectLocation.copy(key = key))
      case (Failure(rootErr), Failure(dirError)) => {
        warn(s"Could not find bag in root: ${rootErr.getMessage}")
        warn(s"Could not find bag in subdir: ${dirError.getMessage}")
        Failure(new IllegalArgumentException("Unable to locate root of bag"))
      }
    }
  }

  private def findBagInfoInRoot(objectLocation: ObjectLocation): Try[String] = {
    val listObjectsResult = s3Client.listObjectsV2(
      objectLocation.namespace,
      createBagInfoPath(objectLocation.key)
    )

    val keyCount = listObjectsResult.getObjectSummaries.size()

    if (keyCount == 1) {
      Success(createBagInfoPath(objectLocation.key))
    } else {
      Failure(new RuntimeException(s"No bag-info.txt inside $objectLocation"))
    }
  }

  private def findBagInfoInDirectory(objectLocation: ObjectLocation): Try[String] = {
    val listObjectsRequest = new ListObjectsV2Request()
      .withBucketName(objectLocation.namespace)
      .withPrefix(objectLocation.key + "/")
      .withDelimiter("/")

    val listObjectsResult: ListObjectsV2Result = s3Client.listObjectsV2(listObjectsRequest)

    val directoriesInBag: Seq[String] = listObjectsResult.getCommonPrefixes.asScala

    if (directoriesInBag.size == 1) {
      val directoryLocation = objectLocation.copy(
        key = directoriesInBag.head
      )

      findBagInfoInRoot(directoryLocation)
    } else {
      Failure(
        new RuntimeException(
          s"Expected exactly one directory in archive; saw <<${listObjectsResult.getCommonPrefixes}>>"
        )
      )
    }
  }

  private def createBagInfoPath(prefix: String): String =
    prefix.stripSuffix("/") + "/bag-info.txt"
}