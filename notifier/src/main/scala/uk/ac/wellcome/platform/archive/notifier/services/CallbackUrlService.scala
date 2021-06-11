package uk.ac.wellcome.platform.archive.notifier.services

import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import io.circe.Printer
import io.circe.syntax._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.ingests.ResponseDisplayIngest
import weco.http.client.HttpClient

import java.net.{URI, URL}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CallbackUrlService(contextUrl: URL, client: HttpClient)(
  implicit ec: ExecutionContext
) extends Logging {

  def buildHttpRequest(ingest: Ingest, callbackUri: URI): HttpRequest = {
    val json = ResponseDisplayIngest(
      ingest = ingest,
      contextUrl = contextUrl
    ).asJson

    val jsonString =
      Printer.noSpaces
        .copy(dropNullValues = true)
        .print(json)

    val entity = HttpEntity(
      contentType = ContentTypes.`application/json`,
      string = jsonString
    )

    debug(s"POST to $callbackUri request:$entity")

    HttpRequest(
      method = HttpMethods.POST,
      uri = callbackUri.toString,
      entity = entity
    )
  }

  def getHttpResponse(
    ingest: Ingest,
    callbackUri: URI
  ): Future[Try[HttpResponse]] =
    client
      .singleRequest(
        buildHttpRequest(ingest, callbackUri)
      )
      .map { Success(_) }
      .recover { case err => Failure(err) }
}
