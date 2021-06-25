package weco.storage_service.bag_root_finder.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service._
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services._
import weco.storage_service.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import weco.storage_service.bag_root_finder.models.{
  RootFinderSuccessSummary,
  RootFinderSummary
}

import scala.concurrent.Future
import scala.util.{Success, Try}

class BagRootFinderWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagRootFinder: BagRootFinder,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  val metricsNamespace: String
)(
  implicit
  val mc: Metrics[Future],
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
        val outgoingPayload = BagRootLocationPayload(
          context = payload.context,
          bagRoot = summary.bagRoot
        )
        outgoingPublisher.sendIfSuccessful(step, outgoingPayload)
      case _ => Success(())
    }
}
