package uk.ac.wellcome.platform.archive.common

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future

abstract class SQSWorkerService(sqsStream: SQSStream[NotificationMessage])
    extends Runnable {
  def run(): Future[Done] =
    sqsStream.foreach(this.getClass.getSimpleName, processMessage)

  def processMessage(notificationMessage: NotificationMessage): Future[Unit]
}
