package uk.ac.wellcome.platform.archive.common

import java.util.UUID

import com.gu.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, Json}

case class IngestID(underlying: UUID) extends AnyVal {
  override def toString: String = underlying.toString
}

object IngestID {
  def random: IngestID = IngestID(UUID.randomUUID())

  implicit val encoder: Encoder[IngestID] = Encoder.instance[IngestID] {
    id: IngestID =>
      Json.fromString(id.toString)
  }

  implicit val decoder: Decoder[IngestID] =
    Decoder.instance[IngestID](cursor => cursor.value.as[UUID].map(IngestID(_)))

  implicit def fmtSpace: DynamoFormat[IngestID] =
    DynamoFormat.coercedXmap[IngestID, String, IllegalArgumentException](
      id => IngestID(UUID.fromString(id))
    )(
      _.toString
    )
}
