package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.Done
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerification
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.progress.models.{
  Progress,
  ProgressEvent,
  ProgressStatusUpdate,
  ProgressUpdate
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class BagVerifierWorkerService(
  notificationStream: NotificationStream[BagRequest],
  verifyDigestFilesService: VerifyDigestFilesService,
  progressSnsWriter: SNSWriter,
  ongoingSnsWriter: SNSWriter,
)(implicit ec: ExecutionContext)
    extends Runnable {

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      tryBagVerification <- verifyBagLocation(bagRequest.bagLocation)

      // We deliberately send to the progress monitor first
      _ <- sendProgressNotification(bagRequest, tryBagVerification)

      _ <- sendOngoingNotification(bagRequest, tryBagVerification)
    } yield ()

  private def verifyBagLocation(
    bagLocation: BagLocation): Future[Try[BagVerification]] =
    verifyDigestFilesService
      .verifyBagLocation(bagLocation)
      .map { bagVerification =>
        Success(bagVerification)
      }
      .recover { case throwable: Throwable => Failure(throwable) }

  private def sendProgressNotification(
    bagRequest: BagRequest,
    tryBagVerification: Try[BagVerification]): Future[PublishAttempt] = {
    val (status, description) = tryBagVerification match {
      case Success(bagVerification) =>
        if (bagVerification.verificationSucceeded) {
          (Progress.Processing, "Successfully verified bag contents")
        } else {
          (
            Progress.Failed,
            "There were problems verifying the bag: not every checksum matched the manifest")
        }
      case _ =>
        (
          Progress.Failed,
          "There were problems verifying the bag: verification could not be performed")
    }

    val progressUpdate = ProgressStatusUpdate(
      id = bagRequest.archiveRequestId,
      status = status,
      affectedBag = None,
      events = List(ProgressEvent(description))
    )

    progressSnsWriter.writeMessage[ProgressUpdate](
      progressUpdate,
      subject = s"Sent by ${this.getClass.getSimpleName}")
  }

  private def sendOngoingNotification(
    bagRequest: BagRequest,
    tryBagVerification: Try[BagVerification]): Future[Unit] =
    tryBagVerification match {
      case Success(bagVerification) if bagVerification.verificationSucceeded =>
        ongoingSnsWriter
          .writeMessage(
            bagRequest,
            subject = s"Sent by ${this.getClass.getSimpleName}"
          )
          .map { _ =>
            ()
          }
      case _ => Future.successful(())
    }
}
