package uk.ac.wellcome.platform.archive.bagverifier.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerification
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressNotice, ProgressUpdate}
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class NotificationService(
                           progressSnsWriter: SNSWriter,
                           outgoingSnsWriter: SNSWriter
                         )(
                         implicit ec: ExecutionContext
) extends Logging {

  def sendProgressNotification(
                                bagRequest: BagRequest,
                                tryBagVerification: Try[BagVerification]
                              ): Future[PublishAttempt] = {

    val progressNotice: ProgressNotice = tryBagVerification match {
      case Success(bagVerification) =>
        info(
          summarizeVerification(
            bagRequest,
            bagVerification
          )
        )

        if (bagVerification.verificationSucceeded) {
          ProgressNotice(
            bagRequest.archiveRequestId,
            "Successfully verified bag contents"
          )
        } else {
          ProgressNotice(
            bagRequest.archiveRequestId,
            Progress.Failed,
            "Problem verifying bag:",
            "File checksum did not match manifest"
          )
        }

      case Failure(exception) =>
        warn(
          List(
            "Verification could not be performed:",
            s"${exception.getMessage} for $bagRequest"
          ).mkString(" ")
        )

        ProgressNotice(
          bagRequest.archiveRequestId,
          Progress.Failed,
          "Problem verifying bag:",
          "Verification could not be performed"
        )
    }

    progressSnsWriter.writeMessage[ProgressUpdate](
      progressNotice.toUpdate(),
      subject = s"Sent by ${this.getClass.getSimpleName}")
  }

  def sendOngoingNotification(
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
