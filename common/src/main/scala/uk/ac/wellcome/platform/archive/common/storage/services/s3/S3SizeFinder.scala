package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.S3Errors
import uk.ac.wellcome.storage.store.NewRetryableReadable

class S3SizeFinder(val maxRetries: Int = 3)(implicit s3Client: AmazonS3)
  extends SizeFinder[S3ObjectLocation]
    with NewRetryableReadable[S3ObjectLocation, Long] {

  override def retryableGetFunction(location: S3ObjectLocation): Long =
    s3Client
      .getObjectMetadata(location.bucket, location.key)
      .getContentLength

  override def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable) match {
      case StoreReadError(exc: AmazonS3Exception)
        if exc.getMessage.startsWith("Not Found") =>
        DoesNotExistError(exc)

      case other => other
    }
}
