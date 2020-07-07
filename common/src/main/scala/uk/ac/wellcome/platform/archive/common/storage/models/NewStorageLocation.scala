package uk.ac.wellcome.platform.archive.common.storage.models

import io.circe.{Decoder, DecodingFailure, HCursor}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, StorageProvider}
import uk.ac.wellcome.storage.{Location, ObjectLocationPrefix, Prefix, S3ObjectLocationPrefix}

sealed trait NewStorageLocation {
  val prefix: Prefix[_ <: Location]
}

sealed trait PrimaryNewStorageLocation extends NewStorageLocation
sealed trait SecondaryNewStorageLocation extends NewStorageLocation

case class PrimaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends PrimaryNewStorageLocation

case class SecondaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends SecondaryNewStorageLocation

trait S3LocationDecodable {
  // (namespace, prefix) -> T
  protected def createDecoder[T](constructor: ObjectLocationPrefix => T, expectedLocationType: String): Decoder[T] =
    (cursor: HCursor) => {
      val oldStyle: Either[DecodingFailure, T] =
        for {
          prefix <- cursor.downField("prefix").as[ObjectLocationPrefix]
          provider <- cursor.downField("provider").as[StorageProvider]
          locationType <- cursor.downField("type").as[String]
          _ = assert(locationType == expectedLocationType)
          _ = assert(provider == AmazonS3StorageProvider)
        } yield constructor(prefix)

      val newStyle: Either[DecodingFailure, T] =
        for {
          prefix <- cursor.downField("prefix").as[S3ObjectLocationPrefix]
        } yield constructor(prefix.toObjectLocationPrefix)

      (oldStyle, newStyle) match {
        case (Right(location), _)       => Right(location)
        case (_, Right(location))       => Right(location)
        case (_, Left(decodingFailure)) => Left(decodingFailure)
      }
    }
}

case object PrimaryNewStorageLocation extends S3LocationDecodable {
  implicit val decoder: Decoder[PrimaryNewStorageLocation] =
    createDecoder(
      (prefix: ObjectLocationPrefix) => PrimaryS3StorageLocation(prefix = S3ObjectLocationPrefix(prefix)),
      expectedLocationType = "PrimaryStorageLocation"
    )
}

case object SecondaryNewStorageLocation extends S3LocationDecodable {
  implicit val decoder: Decoder[SecondaryNewStorageLocation] =
    createDecoder(
      (prefix: ObjectLocationPrefix) => SecondaryS3StorageLocation(prefix = S3ObjectLocationPrefix(prefix)),
      expectedLocationType = "SecondaryStorageLocation"
    )
}
