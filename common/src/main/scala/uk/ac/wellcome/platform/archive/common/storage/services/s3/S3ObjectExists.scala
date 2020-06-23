package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.storage.services.ObjectExists
import uk.ac.wellcome.storage.{ObjectLocation, StoreReadError}

import scala.util.{Failure, Success, Try}

class S3ObjectExists(s3Client: AmazonS3) extends ObjectExists[ObjectLocation] {
  override def exists(
    objectLocation: ObjectLocation
  ): Either[StoreReadError, Boolean] =
    Try {
      s3Client.doesObjectExist(
        objectLocation.namespace,
        objectLocation.path
      )
    } match {
      case Success(exists) => Right(exists)
      case Failure(e)      => Left(StoreReadError(e))
    }
}

object S3ObjectExists {
  implicit class S3ObjectExistsImplicit(objectLocation: ObjectLocation)(
    implicit s3Client: AmazonS3
  ) {
    val s3ObjectExists = new S3ObjectExists(s3Client)

    def exists: Either[StoreReadError, Boolean] =
      s3ObjectExists.exists(objectLocation)
  }
}
