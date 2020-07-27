package uk.ac.wellcome.platform.archive.bagverifier.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.models.{BagVerifyContext, VerificationSummary}
import uk.ac.wellcome.platform.archive.common.VerifiablePayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Try

trait BagPayloadTranslator[
  Payload <: VerifiablePayload,
  BagContext <: BagVerifyContext[BagLocation, BagPrefix],
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation]
] {
  def translate(r: Payload): BagContext
}

class BagVerifierWorker[
  Payload <: VerifiablePayload,
  BagContext <: BagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix],
  IngestDestination,
  OutgoingDestination
](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  verifier: BagVerifier[BagContext, S3ObjectLocation, S3ObjectLocationPrefix],
  val metricsNamespace: String,
  bagPayloadTranslator: BagPayloadTranslator[
    Payload,
    BagContext,
    S3ObjectLocation,
    S3ObjectLocationPrefix
  ]
)(
  implicit val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[Payload]
) extends IngestStepWorker[Payload, VerificationSummary] {

  override def processMessage(
    payload: Payload
  ): Try[IngestStepResult[VerificationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)
      summary <- verifier.verify(
        ingestId = payload.ingestId,
        bagContext = bagPayloadTranslator.translate(payload),
        space = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier
      )
      _ <- ingestUpdater.send(payload.ingestId, summary)
      _ <- outgoingPublisher.sendIfSuccessful(summary, payload)
    } yield summary
}
