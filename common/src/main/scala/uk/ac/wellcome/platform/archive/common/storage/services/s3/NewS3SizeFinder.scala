package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.platform.archive.common.storage.services.NewSizeFinder
import uk.ac.wellcome.storage.s3.S3Errors
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation, ReadError, StoreReadError}
import uk.ac.wellcome.storage.store.RetryableReadable

class NewS3SizeFinder(val maxRetries: Int = 3)(implicit s3Client: AmazonS3)
  extends NewSizeFinder[ObjectLocation]
    with RetryableReadable[Long] {

  override def retryableGetFunction(location: ObjectLocation): Long =
    s3Client
      .getObjectMetadata(location.namespace, location.path)
      .getContentLength

  override def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable) match {
      case StoreReadError(exc: AmazonS3Exception)
        if exc.getMessage.startsWith("Not Found") =>
        DoesNotExistError(exc)

      case other => other
    }
}
