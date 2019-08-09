package uk.ac.wellcome.platform.archive.common.http.models

import java.net.URL

import akka.http.scaladsl.model.StatusCode
import io.circe.generic.extras.JsonKey

case class UserErrorResponse(
  @JsonKey("@context") context: String,
  httpStatus: Int,
  description: String,
  label: String,
  `type`: String = "Error"
)

case object UserErrorResponse {
  def apply(
    context: URL,
    statusCode: StatusCode,
    description: String
  ): UserErrorResponse =
    UserErrorResponse(
      context = context.toString,
      httpStatus = statusCode.intValue(),
      description = description,
      label = statusCode.reason()
    )
}

case class InternalServerErrorResponse(
  @JsonKey("@context") context: String,
  httpStatus: Int,
  label: String,
  `type`: String = "Error"
)

case object InternalServerErrorResponse {
  def apply(context: URL, statusCode: StatusCode): InternalServerErrorResponse =
    InternalServerErrorResponse(
      context = context.toString,
      httpStatus = statusCode.intValue(),
      label = statusCode.reason()
    )
}
