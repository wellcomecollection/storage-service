package uk.ac.wellcome.platform.archive.common.http

import java.net.URL

import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.ExceptionHandler
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.http.models.InternalServerErrorResponse

trait WellcomeExceptionHandler extends Logging {
  import akka.http.scaladsl.server.Directives._
  import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
  import uk.ac.wellcome.json.JsonUtil._

  val httpMetrics: HttpMetrics
  val contextURL: URL

  implicit val exceptionHandler: ExceptionHandler = buildExceptionHandler()

  private def buildExceptionHandler(): ExceptionHandler =
    ExceptionHandler {
      case err: Exception =>
        logger.error(s"Unexpected exception $err")
        val error = InternalServerErrorResponse(
          context = contextURL,
          statusCode = InternalServerError
        )
        httpMetrics.sendMetricForStatus(InternalServerError)
        complete(InternalServerError -> error)
    }
}
