package uk.ac.wellcome.platform.archive.common

import io.circe._
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

case object PipelineMessage {
  def fromJson[T](json: Json)(implicit decoder: Decoder[T]): Either[DecodingFailure, PipelineMessage[T]] =
    for {
      payload <- json.as[T]
    } yield PipelineMessage(json, payload)
}

