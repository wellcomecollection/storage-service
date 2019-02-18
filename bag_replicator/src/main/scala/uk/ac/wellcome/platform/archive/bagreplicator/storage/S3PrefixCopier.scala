package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.s3.transfer.model.CopyResult
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class S3PrefixCopier(s3Client: AmazonS3)(implicit ec: ExecutionContext)
    extends Logging {
  val s3Copier = new S3Copier(s3Client)

  /** Copy all the objects from under ObjectLocation into another ObjectLocation,
    * preserving the relative path from the source in the destination.
    *
    * e.g. if you copy s3://bucket/foo to s3://other_bucket/bar, this
    * function would copy
    *
    *     s3://bucket/foo/0.txt       ~> s3://other_bucket/bar/0.txt
    *     s3://bucket/foo/1.txt       ~> s3://other_bucket/bar/1.txt
    *     s3://bucket/foo/2/20.txt    ~> s3://other_bucket/bar/2/20.txt
    *     s3://bucket/foo/2/21.txt    ~> s3://other_bucket/bar/2/21.txt
    *
    */
  def copyObjects(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation
  ): Future[Unit] =
    for {
      sourceObjects <- listObjectsUnderPrefix(srcLocationPrefix)
      copyObjectPairs = getCopyObjectPairs(
        sourceObjects = sourceObjects,
        srcLocationPrefix = srcLocationPrefix,
        dstLocationPrefix = dstLocationPrefix
      )
      _ <- duplicateObjects(copyObjectPairs)
    } yield ()

  private def listObjects(
    objectLocation: ObjectLocation): Future[ListObjectsV2Result] = {
    val prefix =
      if (objectLocation.key.endsWith("/"))
        objectLocation.key
      else
        objectLocation.key + "/"

    Future {
      val listObjectsResult = s3Client.listObjectsV2(objectLocation.namespace, prefix)

      // @@AWLC We should remove this when we have a fix in place for
      // https://github.com/wellcometrust/platform/issues/3450, but for now it's
      // here to ensure the bag replicator doesn't silently pass bags which it
      // can't copy correctly.
      if (listObjectsResult.isTruncated)
        throw new RuntimeException(
          "ListObjectsV2 result truncated! The replicator couldn't copy everything. " +
          "See https://github.com/wellcometrust/platform/issues/3450"
        )

      listObjectsResult
    }
  }

  private def getObjectLocations(
    listing: ListObjectsV2Result,
    objectLocation: ObjectLocation
  ): List[ObjectLocation] =
    listing.getObjectSummaries.asScala
      .map { summary: S3ObjectSummary =>
        summary.getKey
      }
      .map { key: String =>
        ObjectLocation(
          namespace = objectLocation.namespace,
          key = key
        )
      }
      .toList

  private def listObjectsUnderPrefix(
    locationPrefix: ObjectLocation
  ): Future[List[ObjectLocation]] = {
    // TODO: limit size of the returned List
    // use Marker to paginate(?)
    // needs care if bag contents can change during copy.
    debug(s"listing items in $locationPrefix")

    for {
      listing <- listObjects(locationPrefix)
      summaries = getObjectLocations(listing, locationPrefix)
    } yield summaries
  }

  private def getCopyObjectPairs(
    sourceObjects: List[ObjectLocation],
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation
  ): List[(ObjectLocation, ObjectLocation)] =
    sourceObjects.map { srcLocation =>
      val relativeKey = srcLocation.key
        .stripPrefix(srcLocationPrefix.key)

      val dstLocation = ObjectLocation(
        namespace = dstLocationPrefix.namespace,
        key = dstLocationPrefix.key + relativeKey
      )

      (srcLocation, dstLocation)
    }

  private def duplicateObjects(
    copyObjectPairs: List[(ObjectLocation, ObjectLocation)]
  ): Future[List[CopyResult]] =
    Future.sequence(
      copyObjectPairs.map {
        case (src: ObjectLocation, dst: ObjectLocation) =>
          s3Copier.copy(src = src, dst = dst)
      }
    )
}
