package uk.ac.wellcome.platform.archive.common.ingests.models

import com.gu.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

case class Namespace(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object Namespace {
  implicit val encoder: Encoder[Namespace] = (value: Namespace) => Json.fromString(value.toString)

  implicit val decoder: Decoder[Namespace] = (cursor: HCursor) => cursor.value.as[String].map(Namespace(_))

  implicit def evidence: DynamoFormat[Namespace] =
    DynamoFormat.coercedXmap[Namespace, String, IllegalArgumentException](
      Namespace(_)
    )(
      _.underlying
    )
}
