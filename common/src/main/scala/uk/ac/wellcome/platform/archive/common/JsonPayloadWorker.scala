package uk.ac.wellcome.platform.archive.common

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._

import scala.util.{Failure, Success, Try}

trait JsonPayloadWorker {
  def asPayload[Payload](json: Json)(
    implicit decoder: Decoder[Payload]): Try[Payload] =
    json.as[Payload] match {
      case Right(payload) => Success(payload)
      case Left(err)      => Failure(err)
    }

  def addField[V](json: Json)(key: String, value: V)(
    implicit encoder: Encoder[V]): Json =
    json.deepMerge(Json.obj((key, value.asJson)))
}
