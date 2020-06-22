package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.storage.services.ObjectExists
import uk.ac.wellcome.storage.StoreReadError
import uk.ac.wellcome.storage.s3.S3ObjectLocation

import scala.util.{Failure, Success, Try}

class S3ObjectExists(s3Client: AmazonS3) extends ObjectExists[S3ObjectLocation] {
  override def exists(location: S3ObjectLocation): Either[StoreReadError, Boolean] =
    Try {
      s3Client.doesObjectExist(location.bucket, location.key)
    } match {
      case Success(exists) => Right(exists)
      case Failure(e)      => Left(StoreReadError(e))
    }
}

object S3ObjectExists {
  implicit class S3ObjectExistsImplicit(location: S3ObjectLocation)(
    implicit s3Client: AmazonS3
  ) {
    val s3ObjectExists = new S3ObjectExists(s3Client)

    def exists: Either[StoreReadError, Boolean] =
      s3ObjectExists.exists(location)
  }
}
