package weco.storage.services.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  HeadObjectRequest
}
import weco.storage._
import weco.storage.s3.{S3Errors, S3ObjectLocation}
import weco.storage.services.SizeFinder
import weco.storage.store.RetryableReadable

import scala.util.{Failure, Success, Try}

class S3SizeFinder(val maxRetries: Int = 3)(implicit s3Client: S3Client)
    extends SizeFinder[S3ObjectLocation]
    with RetryableReadable[S3ObjectLocation, Long] {

  override def retryableGetFunction(location: S3ObjectLocation): Long = {
    // We default to using HeadObject, which will return the size immediately
    // on the happy path; we fall back to GetObject if it fails because it gives
    // us more more detailed errors from S3 about why it fails.
    // This helps us pass more informative messages upstream.
    //
    // e.g. HeadObject might return "Bad Request", whereas GetObject will
    // tell us "The bucket name was invalid".
    //
    val headRequest =
      HeadObjectRequest
        .builder()
        .bucket(location.bucket)
        .key(location.key)
        .build()

    Try {
      s3Client
        .headObject(headRequest)
        .contentLength()
    } match {
      case Success(length) => length
      case Failure(_) =>
        val getRequest =
          GetObjectRequest
            .builder()
            .bucket(location.bucket)
            .key(location.key)
            .build()

        val s3Object = s3Client.getObject(getRequest)

        // Note: does it make any sense to look at the contentLength here?
        //
        // Would the GetObject call ever succeed where the HeadObject call failed?
        // If we're only making this call for the quality of the error message, we
        // should already have thrown by this point and this line is unreachable.
        val contentLength = s3Object.response().contentLength()

        // Abort the stream to avoid getting a warning:
        //
        //    Not all bytes were read from the S3ObjectInputStream, aborting
        //    HTTP connection. This is likely an error and may result in
        //    sub-optimal behavior.
        //
        // Note: this warning comes from the AWS Java V1 SDK; I've added
        // an equivalent abort call for V2, but it may no longer be necessary.
        s3Object.abort()

        contentLength
    }
  }

  override def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable)
}
