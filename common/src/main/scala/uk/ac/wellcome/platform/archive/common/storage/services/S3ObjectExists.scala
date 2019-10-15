package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.{ObjectLocation, StorageError, StoreReadError}

import scala.util.{Failure, Success, Try}

trait ObjectExists {
  def exists(objectLocation: ObjectLocation): Either[StorageError, Boolean]
}

class S3ObjectExists(s3Client: AmazonS3) extends ObjectExists {
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

    def exists = s3ObjectExists.exists(objectLocation)
  }
}
