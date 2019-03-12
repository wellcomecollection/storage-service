package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.Done
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.{DiagnosticReporter, OutgoingPublisher}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagVerifierWorker(stream: NotificationStream[BagRequest],
                        ingestUpdater: IngestUpdater,
                        outgoing: OutgoingPublisher,
                        reporter: DiagnosticReporter,
                        verifier: Verifier)(implicit ec: ExecutionContext)
    extends Runnable
    with Logging {

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(request: BagRequest): Future[Unit] = {
    info(s"Received request $request")

    val result = for {
      verification <- verifier.verify(request.bagLocation)

      _ <- reporter.report(request.requestId, verification)
      _ <- ingestUpdater.send(request.requestId, verification)
      _ <- outgoing.send(request.requestId, verification)(_ => request)
    } yield ()

    result
  }
}
