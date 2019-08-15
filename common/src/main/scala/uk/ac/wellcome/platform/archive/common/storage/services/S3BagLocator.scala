package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/** When we get an archive containing a bag, that bag may be the top of the
  * archive, or it might be inside a directory.
  *
  * This class tries to infer the root of the bag, using simple heuristics:
  *
  *   - If there's a bag-info.txt in the root of the archive, that's the
  * root of the bag
  *   - If the archive has a single subdirectory and it contains a bag-info.txt,
  * the subdirectory is the root
  *
  * Anything else is an error.
  *
  * This code is deliberately conservative -- it has the potential to lose
  * data from a bag if we infer incorrectly.
  *
  */
class S3BagLocator(s3Client: AmazonS3) extends Logging {
  def locateBagInfo(prefix: ObjectLocationPrefix): Try[ObjectLocation] = {
    val bagInfoInRoot = findBagInfoInRoot(prefix)
    val bagInfoInDirectory = findBagInfoInDirectory(prefix)

    (bagInfoInRoot, bagInfoInDirectory) match {
      case (Success(path), _) => Success(prefix.copy(path = path).asLocation())
      case (_, Success(path)) => Success(prefix.copy(path = path).asLocation())
      case (Failure(rootErr), Failure(dirError)) => {
        warn(s"Could not find bag in root: ${rootErr.getMessage}")
        warn(s"Could not find bag in subdir: ${dirError.getMessage}")
        Failure(new IllegalArgumentException("Unable to locate root of bag"))
      }
    }
  }

  def locateBagRoot(prefix: ObjectLocationPrefix): Try[ObjectLocationPrefix] =
    locateBagInfo(prefix).map { loc =>
      loc.copy(path = loc.path.stripSuffix("/bag-info.txt")).asPrefix
    }

  /** Find a bag directly below a given ObjectLocation. */
  private def findBagInfoInRoot(prefix: ObjectLocationPrefix): Try[String] =
    Try {
      val listObjectsResult = s3Client.listObjectsV2(
        prefix.namespace,
        createBagInfoPath(prefix.path)
      )

      val keyCount = listObjectsResult.getObjectSummaries.size()

      if (keyCount == 1) {
        createBagInfoPath(prefix.path)
      } else {
        throw new RuntimeException(s"No bag-info.txt inside $prefix")
      }
    }

  /** Look for a subdirectory of the current location, and find a bag-info.txt
    * if there's exactly one such subdirectory.
    *
    */
  private def findBagInfoInDirectory(
    prefix: ObjectLocationPrefix
  ): Try[String] = Try {
    val listObjectsRequest = new ListObjectsV2Request()
      .withBucketName(prefix.namespace)
      .withPrefix(prefix.path + "/")
      .withDelimiter("/")

    val directoriesInBag =
      s3Client.listObjectsV2(listObjectsRequest).getCommonPrefixes.asScala

    if (directoriesInBag.size == 1) {
      val directoryLocation = prefix.copy(
        path = directoriesInBag.head
      )

      findBagInfoInRoot(directoryLocation) match {
        case Success(s) => s
        case Failure(e) => throw e
      }
    } else {
      throw new RuntimeException(
        s"Expected exactly one directory in archive; saw <<$directoriesInBag>>"
      )
    }
  }

  private def createBagInfoPath(prefix: String): String =
    prefix.stripSuffix("/") + "/bag-info.txt"
}
