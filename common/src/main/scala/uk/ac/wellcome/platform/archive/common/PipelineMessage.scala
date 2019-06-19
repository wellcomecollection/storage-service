package uk.ac.wellcome.platform.archive.common

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._

case class PipelineMessage[T](
  json: Json,
  payload: T
) {
  def addField[V](key: String, value: V)(implicit encoder: Encoder[V]): PipelineMessage[T] =
    PipelineMessage(
      json = json.deepMerge(
        Json.obj((key, value.asJson))
      ),
      payload = payload
    )
}

object PipelineMessage {
  implicit def decoder[T](implicit circeDecoder: Decoder[T]): Decoder[PipelineMessage[T]] =
    (cursor: HCursor) => for {
      json <- cursor.as[Json]
      payload <- json.as[T]
    } yield PipelineMessage(json, payload)

  implicit def encoder[T]: Encoder[PipelineMessage[T]] =
    (message: PipelineMessage[T]) => message.json
}
