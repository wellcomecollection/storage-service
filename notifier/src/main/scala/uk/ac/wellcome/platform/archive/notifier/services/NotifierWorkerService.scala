package uk.ac.wellcome.platform.archive.notifier.services

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{NotificationMessage, SNSWriter}
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.common.SQSWorkerService
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorkerService(callbackUrlService: CallbackUrlService,
                            sqsStream: SQSStream[NotificationMessage],
                            snsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends SQSWorkerService[CallbackNotification](sqsStream) {

  def processMessage(callbackNotification: CallbackNotification): Future[Unit] =
    for {
      httpResponse <- callbackUrlService.getHttpResponse(callbackNotification)
      progressUpdate = PrepareNotificationService.prepare(
        callbackNotification.id,
        httpResponse)
      _ <- snsWriter.writeMessage[ProgressUpdate](
        progressUpdate,
        subject = s"Sent by ${this.getClass.getName}"
      )
    } yield ()
}
