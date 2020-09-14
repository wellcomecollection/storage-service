package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, ListObjectsV2Request}
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.s3.{S3Errors, S3ObjectLocation}
import uk.ac.wellcome.storage.store.RetryableReadable

import scala.collection.JavaConverters._

// A variant of S3SizeFinder suitable for looking up the size of lots
// of objects that are adjacent in an S3 bucket.
//
// It gets up to 1000 objects at once with a single ListObjectsV2 call
// and caches the result, rather than individual HeadObject requests.
//
// Note: there is no cache eviction logic; create a new instance of this
// class for every new prefix/collection of objects.
class S3MultiSizeFinder(val maxRetries: Int = 3)(implicit s3Client: AmazonS3)
  extends SizeFinder[S3ObjectLocation]
    with RetryableReadable[S3ObjectLocation, Long] {

  // (bucket) -> Map(key -> size)
  private var cache: Map[S3ObjectLocation, Long] = Map.empty

  override protected def retryableGetFunction(location: S3ObjectLocation): Long =
    cache.get(location) match {
      case Some(size) => size
      case None       =>
        freshenCache(location)

        val exc = new AmazonS3Exception(s"Unable to find size of object $location")
        exc.setStatusCode(404)

        cache.getOrElse(location, throw exc)
    }

  private def freshenCache(location: S3ObjectLocation): Unit = {
    val resp = s3Client.listObjectsV2(
      new ListObjectsV2Request()
        .withBucketName(location.bucket)
        .withStartAfter(location.key.dropRight(1))
    )

    cache = cache ++ resp.getObjectSummaries.asScala
      .map { summary => S3ObjectLocation(location.bucket, summary.getKey) -> summary.getSize }
  }


  override protected def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable)
}
