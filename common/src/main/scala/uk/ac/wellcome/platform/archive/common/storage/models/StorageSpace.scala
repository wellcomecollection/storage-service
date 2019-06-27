package uk.ac.wellcome.platform.archive.common.storage.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

case class StorageSpace(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object StorageSpace {
  implicit val encoder: Encoder[StorageSpace] = (value: StorageSpace) =>
    Json.fromString(value.toString)

  implicit val decoder: Decoder[StorageSpace] = (cursor: HCursor) =>
    cursor.value.as[String].map(StorageSpace(_))

  implicit def evidence: DynamoFormat[StorageSpace] =
    DynamoFormat.iso[StorageSpace, String](
      StorageSpace(_)
    )(
      _.underlying
    )
}
