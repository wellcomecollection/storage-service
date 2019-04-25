package uk.ac.wellcome.platform.archive.common

import java.util.UUID

import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import io.circe.{Decoder, Encoder, Json}
import uk.ac.wellcome.json.JsonUtil.{fromJson, toJson}

case class IngestRequestID(underlying: UUID) extends AnyVal {
  override def toString: String = underlying.toString
}

object IngestRequestID {
  implicit val encoder: Encoder[IngestRequestID] = Encoder.instance[IngestRequestID] {
    id: IngestRequestID => Json.fromString(id.toString)
  }

  implicit val decoder: Decoder[IngestRequestID] = Decoder.instance[IngestRequestID](cursor =>
    cursor.value.as[UUID].map(IngestRequestID(_)))

  implicit def fmtSpace: DynamoFormat[IngestRequestID] =
    DynamoFormat.xmap[IngestRequestID, String](
      fromJson[IngestRequestID](_)(decoder).toEither.left
        .map(TypeCoercionError)
    )(
      toJson[IngestRequestID](_).get
    )
}
