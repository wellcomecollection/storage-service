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
}
