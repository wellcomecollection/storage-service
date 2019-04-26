package uk.ac.wellcome.platform.archive.ingests.services

import java.net.URI

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.CallbackNotification
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}
import uk.ac.wellcome.platform.archive.common.ingests.models.Callback.Pending

import scala.concurrent.{ExecutionContext, Future}

class CallbackNotificationService(snsWriter: SNSWriter)(
  implicit ec: ExecutionContext) {
  def sendNotification(ingest: Ingest): Future[Unit] =
    ingest.callback match {
      case Some(Callback(callbackUri, Pending)) =>
        ingest.status match {
          case Ingest.Completed | Ingest.Failed =>
            sendSnsMessage(callbackUri, ingest = ingest)
          case _ => Future.successful(())
        }
      case _ => Future.successful(())
    }

  private def sendSnsMessage(callbackUri: URI, ingest: Ingest): Future[Unit] = {
    val callbackNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = callbackUri,
      ingest = ingest
    )

    snsWriter
      .writeMessage(
        callbackNotification,
        subject = s"sent by ${this.getClass.getSimpleName}"
      )
      .map { _ =>
        ()
      }
  }
}
