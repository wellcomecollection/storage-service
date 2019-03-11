package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.Done
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.operation.{OperationNotifier, OperationReporter}
import uk.ac.wellcome.typesafe.Runnable
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContext, Future}

class BagVerifierWorker(
                         stream: NotificationStream[BagRequest],
                         notifier: OperationNotifier,
                         reporter: OperationReporter,
                         verifier: Verifier)(implicit ec: ExecutionContext)
    extends Runnable
    with Logging {

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(request: BagRequest): Future[Unit] = {
    info(s"Received request $request")

    val result = for {
      verification <- verifier
        .verify(
          request.bagLocation
        )

      _ <- reporter.report(request.requestId, verification)

      _ <- notifier.send(
        request.requestId,
        verification
      ) { _ =>
        request
      }

    } yield ()

    result
  }
}
