package uk.ac.wellcome.platform.archive.progress_async.services

import java.net.URI

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.Callback.Pending
import uk.ac.wellcome.platform.archive.common.progress.models.{Callback, Progress}

import scala.concurrent.{ExecutionContext, Future}

class CallbackNotificationService(snsWriter: SNSWriter)(implicit ec: ExecutionContext) {
  def sendNotification(progress: Progress): Future[Unit] =
    progress.callback match {
      case Some(Callback(callbackUri, Pending)) =>
        progress.status match {
          case Progress.Completed | Progress.Failed =>
            sendSnsMessage(callbackUri, progress = progress)
          case _ => Future.successful(())
        }
      case _ => Future.successful(())
    }

  private def sendSnsMessage(callbackUri: URI, progress: Progress): Future[Unit] = {
    val callbackNotification = CallbackNotification(
      id = progress.id,
      callbackUri = callbackUri,
      payload = progress
    )

    snsWriter.writeMessage(
      callbackNotification,
      subject = s"sent by ${this.getClass.getSimpleName}"
    ).map { _ => () }
  }
}
