package uk.ac.wellcome.platform.archive.common

import io.circe.{Decoder, Json}

import scala.util.{Failure, Success, Try}

trait JsonPayloadWorker {
  def asPayload[Payload](json: Json)(implicit decoder: Decoder[Payload]): Try[Payload] =
    json.as[Payload] match {
      case Right(payload) => Success(payload)
      case Left(err)      => Failure(err)
    }
}
