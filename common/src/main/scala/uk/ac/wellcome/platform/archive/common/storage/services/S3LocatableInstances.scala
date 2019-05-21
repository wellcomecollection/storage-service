package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import com.amazonaws.services.s3.AmazonS3URI
import uk.ac.wellcome.platform.archive.common.storage.{Locatable, LocateFailure, LocationParsingError}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object S3LocatableInstances {
  implicit val s3UriLocatable = new Locatable[URI] {
    override def locate(t: URI)(maybeRoot: Option[ObjectLocation]): Either[LocateFailure[URI], ObjectLocation] = Try { new AmazonS3URI(t) } match {
      case Success(s3Uri) => Right(ObjectLocation(s3Uri.getBucket, s3Uri.getKey))
      case Failure(e) => Left(LocationParsingError(t, "Failed to parse S3 URI!"))
    }
  }
}
