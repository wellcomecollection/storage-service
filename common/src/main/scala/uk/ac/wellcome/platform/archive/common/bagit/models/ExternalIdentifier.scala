package uk.ac.wellcome.platform.archive.common.bagit.models

import com.gu.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, Json}

case class ExternalIdentifier(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object ExternalIdentifier {
  implicit val encoder: Encoder[ExternalIdentifier] = Encoder.instance[ExternalIdentifier] {
    space: ExternalIdentifier =>
      Json.fromString(space.toString)
  }

  implicit val decoder: Decoder[ExternalIdentifier] = Decoder.instance[ExternalIdentifier](cursor =>
    cursor.value.as[String].map(ExternalIdentifier(_)))

  implicit def evidence: DynamoFormat[ExternalIdentifier] =
    DynamoFormat.coercedXmap[ExternalIdentifier, String, IllegalArgumentException](
      ExternalIdentifier(_)
    )(
      _.underlying
    )
}
