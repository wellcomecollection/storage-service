package uk.ac.wellcome.platform.archive.notifier.services

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

import scala.concurrent.Future

class CallbackUrlService(contextURL: URL)(implicit actorSystem: ActorSystem) extends Logging {
  def getHttpResponse(callbackNotification: CallbackNotification): Future[HttpResponse] = {
    val entity = HttpEntity(
      contentType = ContentTypes.`application/json`,
      string = toJson(ResponseDisplayIngest(callbackNotification.payload, contextURL)).get
    )

    val callbackUri = callbackNotification.callbackUri
    debug(s"POST to $callbackUri request:$entity")
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = callbackUri.toString,
      entity = entity
    )

    Http().singleRequest(request)
  }
}
