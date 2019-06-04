package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSummary
import uk.ac.wellcome.platform.archive.common.BagRootPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

class BagVerifierWorker[IngestDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  verifier: BagVerifier
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {

  private val worker =
    AlpakkaSQSWorker[BagRootPayload, VerificationSummary](
      alpakkaSQSWorkerConfig) { payload =>
      Future.fromTry { processMessage(payload) }
    }

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def processMessage(
    payload: BagRootPayload): Try[Result[VerificationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)
      summary <- verifier.verify(payload.bagRootLocation)
      _ <- ingestUpdater.send(payload.ingestId, summary)
      _ <- outgoingPublisher.sendIfSuccessful(summary, payload)
    } yield toResult(summary)

  override def run(): Future[Any] = worker.start
}
