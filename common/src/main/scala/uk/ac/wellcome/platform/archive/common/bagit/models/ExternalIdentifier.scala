package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

case class ExternalIdentifier(underlying: String) {
  override def toString: String = underlying

  require(!underlying.isEmpty, "External identifier cannot be empty")
}

object ExternalIdentifier {
  implicit val encoder: Encoder[ExternalIdentifier] =
    (value: ExternalIdentifier) => Json.fromString(value.toString)

  implicit val decoder: Decoder[ExternalIdentifier] = (cursor: HCursor) =>
    cursor.value.as[String].map(ExternalIdentifier(_))

  implicit def evidence: DynamoFormat[ExternalIdentifier] =
    DynamoFormat.iso[ExternalIdentifier, String](
      ExternalIdentifier(_)
    )(
      _.underlying
    )
}
