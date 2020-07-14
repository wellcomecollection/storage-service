package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSummary
import uk.ac.wellcome.platform.archive.common.BagRootPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.util.Try

class BagVerifierWorker[R <: BagRootPayload, B <: BagRoot[S3ObjectLocation, S3ObjectLocationPrefix],IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  verifier: BagVerifier[B, S3ObjectLocation, S3ObjectLocationPrefix],
  val metricsNamespace: String,
  f: R => B
)(
  implicit val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[R]
) extends IngestStepWorker[
      R,
      VerificationSummary
    ] {

  override def processMessage(
    payload: R
  ): Try[IngestStepResult[VerificationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)
      summary <- verifier.verify(
        ingestId = payload.ingestId,
        bagRoot = f(payload),
        space = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier
      )
      _ <- ingestUpdater.send(payload.ingestId, summary)
      _ <- outgoingPublisher.sendIfSuccessful(summary, payload)
    } yield summary
}
