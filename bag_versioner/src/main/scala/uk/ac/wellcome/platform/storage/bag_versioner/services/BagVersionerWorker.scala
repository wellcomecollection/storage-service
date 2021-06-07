package uk.ac.wellcome.platform.storage.bag_versioner.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  IngestEvent,
  IngestID,
  IngestVersionUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  VersionedBagRootPayload
}
import uk.ac.wellcome.platform.storage.bag_versioner.models.{
  BagVersionerSuccessSummary,
  BagVersionerSummary
}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}

class BagVersionerWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagVersioner: BagVersioner,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  val metricsNamespace: String
)(
  implicit val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[BagRootLocationPayload]
) extends IngestStepWorker[BagRootLocationPayload, BagVersionerSummary] {

  // The bag versioner can fail if two bags with the same ID are being
  // processed at the same time, and one instance has the lock -- the other
  // instance will fail because it can't acquire a lock.
  //
  // If we're the other instance, we shouldn't retry immediately -- the
  // first instance will still have the lock.  Instead, spin for 30 seconds
  // and only then try again.
  override val visibilityTimeout: Duration = 30.seconds

  override def processMessage(
    payload: BagRootLocationPayload
  ): Try[IngestStepResult[BagVersionerSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      stepResult <- bagVersioner.getSummary(
        ingestId = payload.ingestId,
        ingestDate = payload.ingestDate,
        ingestType = payload.ingestType,
        externalIdentifier = payload.externalIdentifier,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestUpdate(payload.ingestId, stepResult)
      _ <- sendSuccessful(payload)(stepResult)
    } yield stepResult

  private def sendIngestUpdate(
    ingestId: IngestID,
    stepResult: IngestStepResult[BagVersionerSummary]
  ): Try[Unit] =
    stepResult match {
      case IngestStepSucceeded(summary: BagVersionerSuccessSummary, _) =>
        val update = IngestVersionUpdate(
          id = ingestId,
          events = Seq(
            IngestEvent(
              s"${ingestUpdater.stepName.capitalize} succeeded - assigned bag version ${summary.version}"
            )
          ),
          version = summary.version
        )

        ingestUpdater.sendUpdate(update)

      case _ =>
        ingestUpdater.send(ingestId, stepResult)
    }

  private def sendSuccessful(
    payload: BagRootLocationPayload
  )(step: IngestStepResult[BagVersionerSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: BagVersionerSuccessSummary, _) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          VersionedBagRootPayload(
            context = payload.context,
            bagRoot = payload.bagRoot,
            version = summary.version
          )
        )

      case _ => Success(())
    }
}
