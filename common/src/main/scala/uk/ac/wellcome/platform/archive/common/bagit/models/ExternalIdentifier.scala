package uk.ac.wellcome.platform.archive.common.bagit.models

import com.gu.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

case class ExternalIdentifier(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object ExternalIdentifier {
  implicit val encoder: Encoder[ExternalIdentifier] = (value: ExternalIdentifier) => Json.fromString(value.toString)

  implicit val decoder: Decoder[ExternalIdentifier] = (cursor: HCursor) => cursor.value.as[String].map(ExternalIdentifier(_))

  implicit def evidence: DynamoFormat[ExternalIdentifier] =
    DynamoFormat
      .coercedXmap[ExternalIdentifier, String, IllegalArgumentException](
        ExternalIdentifier(_)
      )(
        _.underlying
      )
}
