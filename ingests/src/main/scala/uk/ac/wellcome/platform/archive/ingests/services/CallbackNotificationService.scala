package uk.ac.wellcome.platform.archive.ingests.services

import java.net.URI

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.Callback.Pending
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

import scala.concurrent.{ExecutionContext, Future}

class CallbackNotificationService(snsWriter: SNSWriter)(
  implicit ec: ExecutionContext) {
  def sendNotification(progress: Ingest): Future[Unit] =
    progress.callback match {
      case Some(Callback(callbackUri, Pending)) =>
        progress.status match {
          case Ingest.Completed | Ingest.Failed =>
            sendSnsMessage(callbackUri, progress = progress)
          case _ => Future.successful(())
        }
      case _ => Future.successful(())
    }

  private def sendSnsMessage(callbackUri: URI,
                             progress: Ingest): Future[Unit] = {
    val callbackNotification = CallbackNotification(
      id = progress.id,
      callbackUri = callbackUri,
      payload = progress
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
