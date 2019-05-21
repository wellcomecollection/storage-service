package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage._
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object S3StreamableInstances {

  class StreamableObjectLocation(implicit s3Client: AmazonS3)
    extends Streamable[ObjectLocation, S3ObjectInputStream]
      with Logging {

    private def getObjectContent(location: ObjectLocation) = Try {
      s3Client.getObject(
        location.namespace,
        location.key
      ).getObjectContent
    } match {
      case Success(inputStream) => Right(Some(inputStream))
      case Failure(e) => Left(StreamUnavailable(e.getMessage))
    }

    def stream(location: ObjectLocation): Either[StreamUnavailable, Option[S3ObjectInputStream]] = {
      debug(s"Converting $location to InputStream")

      val bucketExists = Try(s3Client.doesBucketExistV2(location.namespace))
      val objectExists = Try(s3Client.doesObjectExist(location.namespace, location.key))

      val result = (bucketExists, objectExists) match {
        case (Success(true), Success(true)) => getObjectContent(location)
        case (Success(true), Success(false)) => Right(None)
        case (Success(true), Failure(e)) => Left(StreamUnavailable(e.getMessage))
        case (Success(false), _) => Left(StreamUnavailable("The specified bucket is not valid"))
        case (Failure(e), _) => Left(StreamUnavailable(e.getMessage))
      }

      debug(s"Got: $result")

      result
    }
  }

  implicit val locatableObjectLocation = new Locatable[ObjectLocation] {
    override def locate(t: ObjectLocation)(maybeRoot: Option[ObjectLocation]): Either[LocateFailure[ObjectLocation], ObjectLocation] = {
      maybeRoot match {
        case Some(_) => Left(LocationNotFound(t, "Specifying a root location for an ObjectLocation is nonsensical!"))
        case None => Right(t)
      }
    }
  }

  implicit class LocatableStreamable[T](t: T)(implicit s3Client: AmazonS3, locator: Locatable[T]) extends Logging {
    private def locate(root: Option[ObjectLocation]) = for {
      located <- locator.locate(t)(root) match {
        case Left(f) => Left(StreamUnavailable(f.msg))
        case Right(location) => Right(location)
      }

      streamed <- new StreamableObjectLocation().stream(located) match {
        case Left(f: StreamUnavailable) => Left(StreamUnavailable(f.msg))
        case Right(location) => Right(location)
      }
    } yield streamed


    def locate: Either[StreamUnavailable, Option[S3ObjectInputStream]] = {
      debug(s"Attempting to locate Locatable $t")
      val result = locate(None)
      debug(s"Got: ${result}")
      result
    }

    def locateWith(root: ObjectLocation): Either[StreamUnavailable, Option[S3ObjectInputStream]] = {
      debug(s"Attempting to locate Locatable $t")
      val result = locate(Some(root))
      debug(s"Got: ${result}")
      result
    }
  }

}
