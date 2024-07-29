package weco.storage_service.bag_verifier.services

import org.apache.pekko.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.pekko.PekkoSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service.bag_verifier.models.{
  BagVerifyContext,
  VerificationSummary
}
import weco.storage_service.VerifiablePayload
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services.OutgoingPublisher
import weco.storage_service.storage.models.{
  EnsureTrailingSlash,
  IngestStepResult,
  IngestStepWorker
}
import weco.storage.{Location, Prefix}

import scala.concurrent.Future
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
  val config: PekkoSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  verifier: BagVerifier[BagContext, BagLocation, BagPrefix],
  bagPayloadTranslator: BagPayloadTranslator[
    Payload,
    BagContext,
    BagLocation,
    BagPrefix
  ]
)(
  implicit val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[Payload],
  val et: EnsureTrailingSlash[BagPrefix]
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
