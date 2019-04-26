package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSummary
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepWorker
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagVerifierWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater,
  outgoingPublisher: OutgoingPublisher,
  verifier: Verifier
)(implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {

  private val worker: AlpakkaSQSWorker[ObjectLocationPayload, VerificationSummary] =
    AlpakkaSQSWorker[ObjectLocationPayload, VerificationSummary](alpakkaSQSWorkerConfig) {
      processMessage
    }

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def processMessage(payload: ObjectLocationPayload): Future[Result[VerificationSummary]] =
    for {
      verificationSummary: IngestStepResult[VerificationSummary] <- verifier
        .verify(payload.objectLocation)
      _ <- ingestUpdater.send(payload.ingestId, verificationSummary)
      _ <- outgoingPublisher.sendIfSuccessful(verificationSummary, payload)
    } yield toResult(verificationSummary)

  override def run(): Future[Any] = worker.start
}
