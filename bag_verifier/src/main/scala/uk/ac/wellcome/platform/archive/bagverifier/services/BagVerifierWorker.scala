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
import uk.ac.wellcome.platform.archive.common.storage.models.{EnsureTrailingSlash, IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Try

trait BagPayloadTranslator[
  Payload <: VerifiablePayload,
  BagContext <: BagVerifyContext[BagPrefix],
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation]
] {
  def translate(r: Payload): BagContext
}

class BagVerifierWorker[
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation],
  BagContext <: BagVerifyContext[BagPrefix],
  Payload <: VerifiablePayload,
  IngestDestination,
  OutgoingDestination
](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  verifier: BagVerifier[BagContext, BagLocation, BagPrefix],
  val metricsNamespace: String,
  bagPayloadTranslator: BagPayloadTranslator[
    Payload,
    BagContext,
    BagLocation,
    BagPrefix
  ]
)(
  implicit val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[Payload],
 val et : EnsureTrailingSlash[BagPrefix]
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
