package uk.ac.wellcome.platform.archive.notifier.services

import java.net.{URI, URL}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import io.circe.Printer
import io.circe.syntax._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

import scala.concurrent.Future

class CallbackUrlService(contextUrl: URL)(implicit actorSystem: ActorSystem)
    extends Logging {
  implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  def buildHttpRequest(ingest: Ingest,
                        callbackUri: URI): HttpRequest = {
    val jsonString =
      ResponseDisplayIngest(
        ingest = ingest,
        contextUrl = contextUrl
      ).asJson.noSpaces

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

  def getHttpResponse(ingest: Ingest,
                      callbackUri: URI): Future[HttpResponse] =
    Http().singleRequest(
      buildHttpRequest(ingest, callbackUri)
    )
}
