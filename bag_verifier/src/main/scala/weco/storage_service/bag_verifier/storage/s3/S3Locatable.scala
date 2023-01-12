package weco.storage_service.bag_verifier.storage.s3

import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage_service.bag_verifier.storage.{Locatable, LocateFailure, LocationParsingError}

import java.net.URI

object S3Locatable {
  implicit val s3UriLocatable
  : Locatable[S3ObjectLocation, S3ObjectLocationPrefix, URI] =
    new Locatable[S3ObjectLocation, S3ObjectLocationPrefix, URI] {
      override def locate(uri: URI)(
        maybeRoot: Option[S3ObjectLocationPrefix]
      ): Either[LocateFailure[URI], S3ObjectLocation] =
        uri.getScheme match {
          case "s3" => {
            val bucket = uri.getAuthority

            val path = uri.getPath
            val key =
              if (path.length <= 1) { // s3://bucket or s3://bucket/
                ""
              } else { // s3://bucket/key
                // Remove the leading '/'.
                uri.getPath.substring(1)
              }

            Right(S3ObjectLocation(bucket, key))
          }

          case "http" if uri.getHost == "localhost" =>
            uri.getPath.split("/").toList match {
              case _ :: head :: tail =>
                Right(S3ObjectLocation(bucket = head, key = tail.mkString("/")))
              case default =>
                Left(
                  LocationParsingError(
                    uri,
                    s"Failed to parse S3 URI: invalid path trying to parse local URL (${default
                      .mkString("/")})"
                  )
                )
            }

          case _ => Left(LocationParsingError(uri, "Failed to parse S3 URI"))
        }
    }
}
