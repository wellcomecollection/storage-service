package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.FilterInputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage._
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class S3ObjectStream(inputStream: S3ObjectInputStream, val contentLength: Long)
    extends FilterInputStream(inputStream)

object S3StreamableInstances {

  class StreamableObjectLocation(implicit s3Client: AmazonS3)
      extends Streamable[ObjectLocation, S3ObjectStream]
      with Logging {

    private def getObjectContent(location: ObjectLocation)
      : Either[StreamUnavailable, Some[S3ObjectStream]] = {
      val result = for {
        s3Object <- Try {
          s3Client
            .getObject(
              location.namespace,
              location.path
            )
        }
        contentLength <- Try {
          s3Object.getObjectMetadata.getContentLength
        }
        inputStream <- Try {
          s3Object.getObjectContent
        }
        stream = new S3ObjectStream(inputStream, contentLength = contentLength)
      } yield stream

      result match {
        case Success(stream) => Right(Some(stream))
        case Failure(err) => Left(StreamUnavailable(err.getMessage))
      }
    }

    def stream(location: ObjectLocation)
      : Either[StreamUnavailable, Option[S3ObjectStream]] = {
      debug(s"Converting $location to InputStream")

      val bucketExists = Try(s3Client.doesBucketExistV2(location.namespace))
      val objectExists = Try(
        s3Client.doesObjectExist(location.namespace, location.path))

      val result = (bucketExists, objectExists) match {
        case (Success(true), Success(true)) => getObjectContent(location)
        case (Success(true), Success(false)) => Right(None)
        case (Success(true), Failure(e)) =>
          Left(StreamUnavailable(e.getMessage))
        case (Success(false), _) =>
          Left(StreamUnavailable("The specified bucket is not valid"))
        case (Failure(e), _) => Left(StreamUnavailable(e.getMessage))
      }

      debug(s"Got: $result")

      result
    }
  }

  // An object location can be resolved as itself
  implicit val locatableObjectLocation = new Locatable[ObjectLocation] {
    override def locate(t: ObjectLocation)(maybeRoot: Option[ObjectLocation])
      : Either[LocateFailure[ObjectLocation], ObjectLocation] = {
      maybeRoot match {
        case Some(_) =>
          Left(
            LocationNotFound(
              t,
              "Specifying a root location for an ObjectLocation is nonsensical!")
          )
        case None => Right(t)
      }
    }
  }

  implicit class S3StreamableOps[T](t: T)(implicit s3Client: AmazonS3,
                                          locator: Locatable[T])
      extends Logging {
    private def locate(root: Option[ObjectLocation])
      : Either[StreamUnavailable, Option[S3ObjectStream]] =
      for {
        located <- locator.locate(t)(root) match {
          case Left(f) => Left(StreamUnavailable(f.msg))
          case Right(location) => Right(location)
        }

        streamed <- new StreamableObjectLocation().stream(located) match {
          case Left(f: StreamUnavailable) => Left(StreamUnavailable(f.msg))
          case Right(location) => Right(location)
        }
      } yield streamed

    def toInputStream: Either[StreamUnavailable, Option[S3ObjectStream]] = {
      debug(s"Attempting to locate Locatable $t")
      val result = locate(root = None)
      debug(s"Got: $result")
      result
    }

    def locateWith(root: ObjectLocation)
      : Either[StreamUnavailable, Option[S3ObjectStream]] = {
      debug(s"Attempting to locate Locatable $t")
      val result = locate(Some(root))
      debug(s"Got: $result")
      result
    }
  }
}
