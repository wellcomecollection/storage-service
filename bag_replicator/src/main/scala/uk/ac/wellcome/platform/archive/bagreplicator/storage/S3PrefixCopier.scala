package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class S3PrefixCopier(s3Client: AmazonS3)(implicit ec: ExecutionContext)
    extends Logging {

  val transferManager: TransferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

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
  ): Future[Unit] = Future {
    val objects: Iterator[S3ObjectSummary] = S3Objects
      .withPrefix(s3Client, srcLocationPrefix.namespace, srcLocationPrefix.key)
      .iterator()
      .asScala

    // Implementation note: this means we're single-threaded within a single bag.
    // That is, we're copying objects in a bag one at a time.
    //
    // We could rewrite this code to process objects in parallel, but it would
    // make it more complicated and introduces more failure modes.  For now we'll
    // just use this simple version, and we can revisit it if it's not fast
    // enough in practice.

    while (objects.hasNext) {
      val summary = objects.next()

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

      s3Copier.copy(srcLocation, dstLocation)
    }
  }
}
