package weco.storage.listing.s3

import grizzled.slf4j.Logging
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, S3Object}
import weco.storage.ListingFailure
import weco.storage.providers.s3.S3ObjectLocationPrefix

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3ObjectListing()(implicit s3Client: S3Client)
    extends S3Listing[S3Object]
    with Logging {
  override def list(prefix: S3ObjectLocationPrefix): ListingResult = {
    if (!prefix.keyPrefix.endsWith("/") && prefix.keyPrefix != "") {
      warn(
        "Listing an S3 prefix that does not end with a slash " +
          s"($prefix) may return unexpected objects. " +
          "See https://alexwlchan.net/2020/08/s3-prefixes-are-not-directories/"
      )
    }

    Try {
      val listRequest =
        ListObjectsV2Request
          .builder()
          .bucket(prefix.bucket)
          .prefix(prefix.keyPrefix)
          .build()

      val iterator =
        s3Client
          .listObjectsV2Paginator(listRequest)
          .contents()
          .iterator()
          .asScala
          .toIterable

      // Because the iterator is lazy, it won't make the initial call to S3 until
      // the caller starts to consume the results.  This can cause an exception to
      // be thrown in user code if, for example, the bucket doesn't exist.
      //
      // Although we discard the result of this toString method immediately, it
      // causes an exception to be thrown here and a Left returned, rather than
      // bubbling up the exception in user code.
      //
      // See the test cases in S3ListingTestCases.
      iterator.toString()

      iterator
    } match {
      case Failure(err)     => Left(ListingFailure(prefix, err))
      case Success(objects) => Right(objects)
    }
  }
}
