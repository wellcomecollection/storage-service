package uk.ac.wellcome.platform.archive.common

import java.util.UUID

import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import io.circe.{Decoder, Encoder, Json}
import uk.ac.wellcome.json.JsonUtil.{fromJson, toJson}

case class IngestID(underlying: UUID) extends AnyVal {
  override def toString: String = underlying.toString
}

object IngestID {
  def random: IngestID = IngestID(UUID.randomUUID())

  implicit val encoder: Encoder[IngestID] = Encoder.instance[IngestID] {
    id: IngestID => Json.fromString(id.toString)
  }

  implicit val decoder: Decoder[IngestID] = Decoder.instance[IngestID](cursor =>
    cursor.value.as[UUID].map(IngestID(_)))

  implicit def fmtSpace: DynamoFormat[IngestID] =
    DynamoFormat.xmap[IngestID, String](
      fromJson[IngestID](_)(decoder).toEither.left
        .map(TypeCoercionError)
    )(
      toJson[IngestID](_).get
    )
}
