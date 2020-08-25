package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.{S3Errors, S3ObjectLocation}
import uk.ac.wellcome.storage.store.RetryableReadable

import scala.util.{Failure, Success, Try}

class S3SizeFinder(val maxRetries: Int = 3)(implicit s3Client: AmazonS3)
    extends SizeFinder[S3ObjectLocation]
    with RetryableReadable[S3ObjectLocation, Long] {

  override def retryableGetFunction(location: S3ObjectLocation): Long = {
    // We default to using getObjectMetadata, which will return the size
    // immediately on the happy path; we fall back to getObject if it fails
    // because it gives us more more detailed errors from S3 about why
    // a GetObject fails.  This helps us pass more informative messages upstream.
    //
    // e.g. GetObject will return "The bucket name was invalid" rather than
    // "Bad Request".
    //
    Try {
      s3Client
        .getObjectMetadata(location.bucket, location.key)
        .getContentLength
    } match {
      case Success(length) => length
      case Failure(_) =>
        val s3Object = s3Client.getObject(
          new GetObjectRequest(location.bucket, location.key)
        )
        val metadata = s3Object.getObjectMetadata
        val contentLength = metadata.getContentLength

        // Abort the stream to avoid getting a warning:
        //
        //    Not all bytes were read from the S3ObjectInputStream, aborting
        //    HTTP connection. This is likely an error and may result in
        //    sub-optimal behavior.
        //
        s3Object.getObjectContent.abort()
        s3Object.getObjectContent.close()

        contentLength
    }
  }

  override def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable)
}
