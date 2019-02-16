package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class S3PrefixCopier(s3Client: AmazonS3)(implicit ec: ExecutionContext) extends Logging {
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
      _ <- duplicateObjects(
        sourceObjects = sourceObjects,
        srcLocationPrefix = srcLocationPrefix,
        dstLocationPrefix = dstLocationPrefix
     )
    } yield ()

  private def listObjects(objectLocation: ObjectLocation): Future[ObjectListing] = {
    val prefix = if (objectLocation.key.endsWith("/"))
      objectLocation.key
    else
      objectLocation.key + "/"

    Future {
      s3Client.listObjects(objectLocation.namespace, prefix)
    }
  }

  private def getObjectLocations(
    listing: ObjectListing,
    objectLocation: ObjectLocation
  ): Future[List[ObjectLocation]] = Future {
    listing.getObjectSummaries.asScala
      .map { summary: S3ObjectSummary => summary.getKey }
      .map { key: String =>
        ObjectLocation(
          namespace = objectLocation.namespace,
          key = key
        )
      }
      .toList
  }

  private def listObjectsUnderPrefix(
    locationPrefix: ObjectLocation
  ): Future[List[ObjectLocation]] = {
    // TODO: limit size of the returned List
    // use Marker to paginate(?)
    // needs care if bag contents can change during copy.
    debug(s"listing items in $locationPrefix")

    for {
      listing <- listObjects(locationPrefix)
      summaries <- getObjectLocations(listing, locationPrefix)
    } yield summaries
  }

  private def duplicateObjects(
    sourceObjects: List[ObjectLocation],
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation
  ): Future[List[ObjectLocation]] = {
    debug(s"duplicating S3 objects: $sourceObjects")

    Future.sequence(
      sourceObjects.map { srcLocation =>
        val relativeKey = srcLocation.key.replaceFirst(srcLocationPrefix.key, "")

        val dstLocation = ObjectLocation(
          namespace = dstLocationPrefix.namespace,
          key = dstLocationPrefix.key + relativeKey
        )

        val future = s3Copier.copy(
          src = srcLocation,
          dst = dstLocation
        )

        future.map { _ => dstLocation }
      }
    )
  }
}