package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.{ExecutionContext, Future}

class S3PrefixCopier(
  s3Client: AmazonS3,
  copier: ObjectCopier,
  batchSize: Int = 1000
)(implicit ec: ExecutionContext)
    extends Logging {

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
  ): Future[Unit] = Future {
    val objects: ParSeq[S3ObjectSummary] = S3Objects
      .withPrefix(
        s3Client,
        srcLocationPrefix.namespace,
        srcLocationPrefix.key
      )
      .withBatchSize(batchSize)
      .iterator()
      .asScala
      .toStream
      .par

    objects.foreach { summary: S3ObjectSummary =>
      val srcLocation = ObjectLocation(
        namespace = srcLocationPrefix.namespace,
        key = summary.getKey
      )

      val relativeKey = srcLocation.key
        .stripPrefix(srcLocationPrefix.key)

      val dstLocation = ObjectLocation(
        namespace = dstLocationPrefix.namespace,
        key = dstLocationPrefix.key + relativeKey
      )

      copier.copy(srcLocation, dstLocation)
    }
  }
}
