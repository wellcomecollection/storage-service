package uk.ac.wellcome.platform.archive.notifier.services

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.CallbackNotification
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CallbackUrlService(contextUrl: URL)(implicit actorSystem: ActorSystem,
                                          ec: ExecutionContext)
    extends Logging {
  def getHttpResponse(
    callbackNotification: CallbackNotification): Future[Try[HttpResponse]] = {
    for {
      jsonString <- Future.fromTry(
        toJson(ResponseDisplayIngest(
          ingest = callbackNotification.payload,
          contextUrl = contextUrl
        ))
      )
      entity <- HttpEntity(
        contentType = ContentTypes.`application/json`,
        string = jsonString
      )

      callbackUri = callbackNotification.callbackUri
      _ = debug(s"POST to $callbackUri request:$entity")

      request <- HttpRequest(
        method = HttpMethods.POST,
        uri = callbackUri.toString,
        entity = entity
      )
    } yield Http().singleRequest(request)
  }.map { resp =>
      Success(resp)
    }
    .recover { case err => Failure(err) }
}
