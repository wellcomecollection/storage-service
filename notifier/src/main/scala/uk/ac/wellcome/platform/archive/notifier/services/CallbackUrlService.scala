package uk.ac.wellcome.platform.archive.notifier.services

import java.net.{URI, URL}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CallbackUrlService(contextUrl: URL)(implicit actorSystem: ActorSystem,
                                          ec: ExecutionContext)
    extends Logging {
  def getHttpResponse(
    ingest: Ingest,
    callbackUri: URI): Future[Try[HttpResponse]] = {
    for {
      jsonString <- Future.fromTry(
        toJson(
          ResponseDisplayIngest(
            ingest = ingest,
            contextUrl = contextUrl
          ))
      )
      entity = HttpEntity(
        contentType = ContentTypes.`application/json`,
        string = jsonString
      )

      _ = debug(s"POST to $callbackUri request:$entity")

      request = HttpRequest(
        method = HttpMethods.POST,
        uri = callbackUri.toString,
        entity = entity
      )
      response <- Http().singleRequest(request)
    } yield Success(response)
  }.recover { case err => Failure(err) }
}
