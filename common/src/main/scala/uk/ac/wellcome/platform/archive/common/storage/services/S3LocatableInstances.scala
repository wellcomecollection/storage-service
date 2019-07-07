package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import com.amazonaws.services.s3.AmazonS3URI
import uk.ac.wellcome.platform.archive.common.storage.{
  Locatable,
  LocateFailure,
  LocationParsingError
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object S3LocatableInstances {
  implicit val s3UriLocatable = new Locatable[URI] {
    override def locate(t: URI)(maybeRoot: Option[ObjectLocation])
      : Either[LocateFailure[URI], ObjectLocation] =
      Try { new AmazonS3URI(t) } match {
        case Success(s3Uri) =>
          Right(ObjectLocation(s3Uri.getBucket, s3Uri.getKey))

        // We are not running in AWS - manually parse URL
        case Failure(_) if t.getHost == "localhost" =>
          t.getPath.split("/").toList match {
            case _ :: head :: tail =>
              Right(ObjectLocation(head, tail.mkString("/")))
            case default =>
              Left(
                LocationParsingError(
                  t,
                  s"Failed to parse S3 URI: invalid path trying to parse local URL (${default
                    .mkString("/")})"))
          }

        // We are running in AWS - fail as usual.
        case Failure(e) =>
          Left(
            LocationParsingError(
              t,
              s"Failed to parse S3 URI: ${e.getMessage}"))

      }
  }
}
