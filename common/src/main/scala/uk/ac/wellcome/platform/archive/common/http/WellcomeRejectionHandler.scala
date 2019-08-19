package uk.ac.wellcome.platform.archive.common.http

import java.net.URL

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpResponse,
  MessageEntity,
  StatusCode
}
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.{
  MalformedRequestContentRejection,
  RejectionHandler,
  StandardRoute
}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import io.circe.CursorOp
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}

import scala.concurrent.ExecutionContext

trait WellcomeRejectionHandler {
  import akka.http.scaladsl.server.Directives._
  import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
  import uk.ac.wellcome.json.JsonUtil._

  val httpMetrics: HttpMetrics
  val contextURL: URL

  implicit val ec: ExecutionContext

  implicit val rejectionHandler: RejectionHandler = buildRejectionHandler()

  private def buildRejectionHandler(): RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case MalformedRequestContentRejection(_, causes: DecodingFailures) =>
          handleDecodingFailures(causes)
      }
      .result()
      .seal
      .mapRejectionResponse {
        case res @ HttpResponse(
              statusCode,
              _,
              HttpEntity.Strict(contentType, _),
              _
            ) if contentType != ContentTypes.`application/json` =>
          transformToJsonErrorResponse(statusCode, res)
        case x => x
      }
      .mapRejectionResponse { resp: HttpResponse =>
        httpMetrics.sendMetric(resp)
        resp
      }

  private def handleDecodingFailures(
    causes: DecodingFailures
  ): StandardRoute = {
    val message = causes.failures.map { cause =>
      val path = CursorOp.opsToPath(cause.history)

      // Error messages returned by Circe are somewhat inconsistent and we also return our
      // own error messages when decoding enums (DisplayIngestType and DisplayStorageProvider).
      val reason = cause.message match {
        // "Attempt to decode value on failed cursor" seems to mean in circeworld
        // that a required field was not present.
        case s if s.contains("Attempt to decode value on failed cursor") =>
          "required property not supplied."
        // These are errors returned by our custom decoders for enum.
        case s if s.contains("valid values") => s
        // If a field exists in the JSON but it's of the wrong format
        // (for example the schema says it should be a String but an object has
        // been supplied instead), the error message returned by Circe only
        // contains the expected type.
        case s => s"should be a $s."
      }

      s"Invalid value at $path: $reason"
    }

    complete(
      BadRequest -> UserErrorResponse(
        context = contextURL,
        statusCode = BadRequest,
        description = message.toList.mkString("\n")
      )
    )
  }

  private def transformToJsonErrorResponse(
    statusCode: StatusCode,
    response: HttpResponse
  ): HttpResponse = {

    val errorResponseMarshallingFlow = Flow[ByteString]
      .mapAsync(parallelism = 1)(data => {
        val description = data.utf8String
        if (statusCode.intValue() >= 500) {
          val response = InternalServerErrorResponse(
            context = contextURL,
            statusCode = statusCode
          )
          Marshal(response).to[MessageEntity]
        } else {
          val response = UserErrorResponse(
            context = contextURL,
            statusCode = statusCode,
            description = description
          )
          Marshal(response).to[MessageEntity]
        }
      })
      .flatMapConcat(_.dataBytes)

    response
      .transformEntityDataBytes(errorResponseMarshallingFlow)
      .mapEntity(
        entity => entity.withContentType(ContentTypes.`application/json`)
      )
  }
}
