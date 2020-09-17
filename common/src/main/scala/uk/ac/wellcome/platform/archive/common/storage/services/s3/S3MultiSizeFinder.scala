package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
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
// This means that the first lookup is slow (ListObjectsV2 call), but
// subsequent lookups are much faster.
//
// e.g. GetSize(a1)    -> (call ListObjects, cache results of a1, ..., a1000)
//                        return size of a1
//      GetSize(a2)    -> return cached size of a2
//      ...
//      GetSize(a1000) -> return cached size of a1000
//      GetSize(a1001) -> (call ListObjects, cache results of a1001, ..., a1100)
//                        return size of 1001
//
// Note: there is no cache eviction logic; create a new instance of this
// class for every new prefix/collection of objects.
//
class S3MultiSizeFinder(val maxRetries: Int = 3)(implicit s3Client: AmazonS3)
  extends SizeFinder[S3ObjectLocation]
    with RetryableReadable[S3ObjectLocation, Long] {

  private var sizeCache: Map[S3ObjectLocation, Long] = Map.empty

  private val fallback = new S3SizeFinder(maxRetries = maxRetries)

  override protected def retryableGetFunction(location: S3ObjectLocation): Long =
    sizeCache.get(location) match {
      case Some(size) => size
      case None       =>
        freshenCache(location)

        // It's possible that a ListObjectsV2 result might miss this key --
        // if there are lots of keys between our StartAfter and the key we're
        // actually interested in -- so if it's not found, make a second
        // HeadObject request just in case.
        //
        // e.g. object-a1, object-a2, ..., object-a1000, object-b
        //
        // For getSize(object-b), calling ListObjects(StartAfter=object-) would
        // find all the object-a* sizes, but not the thing we're interested in!
        sizeCache.getOrElse(location, fallback.retryableGetFunction(location))
    }

  private def freshenCache(location: S3ObjectLocation): Unit = {
    val resp = s3Client.listObjectsV2(
      new ListObjectsV2Request()
        .withBucketName(location.bucket)
        // The StartAfter parameter includes keys after *but not including*
        // whatever you specify here.
        //
        // e.g. List(StartAfter="bagit.txt") doesn't actually see "bagit.txt".
        //
        // Truncating the key is a way to (hopefully) capture the key
        // we're interested in.
        .withStartAfter(location.key.dropRight(1))
    )

    sizeCache = sizeCache ++ resp.getObjectSummaries.asScala
      .map { summary => S3ObjectLocation(location.bucket, summary.getKey) -> summary.getSize }
  }


  override protected def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable)
}
