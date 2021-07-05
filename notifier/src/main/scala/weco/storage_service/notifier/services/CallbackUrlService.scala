package weco.storage_service.notifier.services

import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import weco.http.client.HttpClient
import weco.http.json.DisplayJsonUtil
import weco.storage_service.display.ingests.ResponseDisplayIngest
import weco.storage_service.ingests.models.Ingest

import java.net.URI
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CallbackUrlService(client: HttpClient)(
  implicit ec: ExecutionContext
) extends Logging with DisplayJsonUtil {

  def buildHttpRequest(ingest: Ingest, callbackUri: URI): HttpRequest = {
    val entity = HttpEntity(
      contentType = ContentTypes.`application/json`,
      string = toJson(ResponseDisplayIngest(ingest))
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
