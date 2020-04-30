package uk.ac.wellcome.platform.storage.bag_root_finder.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepSucceeded, IngestStepWorker}
import uk.ac.wellcome.platform.storage.bag_root_finder.models.{RootFinderSuccessSummary, RootFinderSummary}

import scala.util.{Success, Try}

class BagRootFinderWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagRootFinder: BagRootFinder,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  val metricsNamespace: String
)(
  implicit
  val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[UnpackedBagLocationPayload]
) extends IngestStepWorker[UnpackedBagLocationPayload, RootFinderSummary] {

  override def processMessage(
    payload: UnpackedBagLocationPayload
  ): Try[IngestStepResult[RootFinderSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      summary <- bagRootFinder.getSummary(
        ingestId = payload.ingestId,
        unpackLocation = payload.unpackedBagLocation
      )

      _ <- ingestUpdater.send(payload.ingestId, summary)
      _ <- sendSuccessful(payload)(summary)
    } yield summary

  private def sendSuccessful(
    payload: PipelinePayload
  )(step: IngestStepResult[RootFinderSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: RootFinderSuccessSummary, _) =>
        val outgoingPayload: BagRootPayload = BagRootLocationPayload(
          context = payload.context,
          bagRoot = summary.rootLocation
        )
        outgoingPublisher.sendIfSuccessful(step, outgoingPayload)
      case _ => Success(())
    }
}
