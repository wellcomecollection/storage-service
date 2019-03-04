package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.Done
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerification
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class BagVerifierWorkerService(
  notificationStream: NotificationStream[BagRequest],
  verifyDigestFilesService: VerifyDigestFilesService,
  notificationService: NotificationService
)(implicit ec: ExecutionContext)
    extends Runnable
    with Logging {

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] = {
    info(s"Received request $bagRequest")

    val result = for {
      tryBagVerification <- verifyBagLocation(
        bagRequest.bagLocation
      )

      _ <- notificationService
        .sendProgressNotification(
          bagRequest,
          tryBagVerification
        )

      _ <- notificationService
        .sendOutgoingNotification(
          bagRequest,
          tryBagVerification
        )
    } yield ()

    result
  }

  private def verifyBagLocation(
    bagLocation: BagLocation
  ): Future[Try[BagVerification]] =
    verifyDigestFilesService
      .verifyBagLocation(bagLocation)
      .map(Success(_))
      .recover {
        case throwable: Throwable =>
          error(s"Failed for $bagLocation")
          Failure(throwable)
      }

  private def sendProgressNotification(
    bagRequest: BagRequest,
    tryBagVerification: Try[BagVerification]): Future[PublishAttempt] = {
    val (status, description) = tryBagVerification match {
      case Success(bagVerification) =>
        info(summarizeVerification(bagRequest, bagVerification))
        if (bagVerification.verificationSucceeded) {
          (Progress.Processing, "Successfully verified bag contents")
        } else {
          (
            Progress.Failed,
            "There were problems verifying the bag: not every checksum matched the manifest")
        }
      case Failure(exception) =>
        warn(
          f"verification could not be performed:${exception.getMessage} for $bagRequest")
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
        outgoingSnsWriter
          .writeMessage(
            bagRequest,
            subject = s"Sent by ${this.getClass.getSimpleName}"
          )
          .map { _ =>
            ()
          }
      case _ => Future.successful(())
    }

  private def summarizeVerification(
    bagRequest: BagRequest,
    bagVerification: BagVerification): String = {
    val verificationStatus = if (bagVerification.verificationSucceeded) {
      "successful"
    } else {
      "failed"
    }
    f"""$verificationStatus verification
       |of ${bagRequest.bagLocation.completePath}
       |completed in ${bagVerification.duration.getSeconds}s
       | :
       |${bagVerification.successfulVerifications.size} succeeded /
       |${bagVerification.failedVerifications.size} failed
       """.stripMargin.replaceAll("\n", " ")
  }

}
