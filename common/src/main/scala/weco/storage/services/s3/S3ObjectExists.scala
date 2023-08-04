package weco.storage.services.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, S3Exception}
import weco.storage.StoreReadError
import weco.storage.s3.S3ObjectLocation
import weco.storage.services.ObjectExists

import scala.util.{Failure, Success, Try}

class S3ObjectExists(s3Client: S3Client)
    extends ObjectExists[S3ObjectLocation] {
  override def exists(
    location: S3ObjectLocation
  ): Either[StoreReadError, Boolean] = {
    val headRequest =
      HeadObjectRequest
        .builder()
        .bucket(location.bucket)
        .key(location.key)
        .build()

    Try {
      s3Client.headObject(headRequest)
    } match {
      case Success(exists)                                  => Right(true)
      case Failure(e: S3Exception) if e.statusCode() == 404 => Right(false)
      case Failure(e)                                       => Left(StoreReadError(e))
    }
  }
}

object S3ObjectExists {

  implicit class S3ObjectExistsImplicit(location: S3ObjectLocation)(
    implicit s3Client: S3Client
  ) {
    val s3ObjectExists = new S3ObjectExists(s3Client)

    def exists: Either[StoreReadError, Boolean] =
      s3ObjectExists.exists(location)
  }

}
