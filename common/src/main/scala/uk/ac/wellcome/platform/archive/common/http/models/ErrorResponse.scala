package uk.ac.wellcome.platform.archive.common.http.models

import java.net.URL

import akka.http.scaladsl.model.StatusCode
import io.circe.generic.extras.JsonKey

trait ErrorResponse {
  val context: String
  val httpStatus: Int
  val label: String
  val `type`: String
}

case class UserErrorResponse(
  @JsonKey("@context") context: String,
  httpStatus: Int,
  description: String,
  label: String,
  `type`: String = "Error"
) extends ErrorResponse

case class InternalServerErrorResponse(
  @JsonKey("@context") context: String,
  httpStatus: Int,
  label: String,
  `type`: String = "Error"
) extends ErrorResponse

case object ErrorResponse {
  def apply(context: URL,
            statusCode: StatusCode,
            description: String): ErrorResponse =
    if (statusCode.intValue() >= 500) {
      InternalServerErrorResponse(
        context = context.toString,
        httpStatus = statusCode.intValue(),
        label = statusCode.reason()
      )
    } else {
      UserErrorResponse(
        context = context.toString,
        httpStatus = statusCode.intValue(),
        description = description,
        label = statusCode.reason()
      )
    }
}
