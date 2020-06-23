package uk.ac.wellcome.platform.archive.bagverifier.storage.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3URI
import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocateFailure,
  LocationParsingError
}
import uk.ac.wellcome.storage.S3ObjectLocation

import scala.util.{Failure, Success, Try}

object S3Locatable {
  implicit val s3UriLocatable: Locatable[S3ObjectLocation, URI] =
    new Locatable[S3ObjectLocation, URI] {
      override def locate(t: URI)(
        maybeRoot: Option[S3ObjectLocation]
      ): Either[LocateFailure[URI], S3ObjectLocation] =
        Try { new AmazonS3URI(t) } match {
          case Success(s3Uri) =>
            Right(
              S3ObjectLocation(bucket = s3Uri.getBucket, key = s3Uri.getKey)
            )

          // We are not running in AWS - manually parse URL
          case Failure(_) if t.getHost == "localhost" =>
            t.getPath.split("/").toList match {
              case _ :: head :: tail =>
                Right(S3ObjectLocation(bucket = head, key = tail.mkString("/")))
              case default =>
                Left(
                  LocationParsingError(
                    t,
                    s"Failed to parse S3 URI: invalid path trying to parse local URL (${default
                      .mkString("/")})"
                  )
                )
            }

          // We are running in AWS - fail as usual.
          case Failure(e) =>
            Left(
              LocationParsingError(
                t,
                s"Failed to parse S3 URI: ${e.getMessage}"
              )
            )
        }
    }
}
