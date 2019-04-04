package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.ArchiveLocationException
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagRequest,
  UnpackBagRequest
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.models.{
  WorkerFailed,
  WorkerResult,
  WorkerSucceeded
}
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

case class BagUnpackerWorker(alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
                             bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
                             ingestUpdater: IngestUpdater,
                             outgoingPublisher: OutgoingPublisher,
                             unpacker: Unpacker)(implicit ac: ActorSystem,
                                                 ex: ExecutionContext,
                                                 mc: MonitoringClient,
                                                 sc: AmazonSQSAsync)
    extends Runnable
    with Logging {

  private val worker: AlpakkaSQSWorker[UnpackBagRequest, UnpackSummary] =
    AlpakkaSQSWorker[UnpackBagRequest, UnpackSummary](alpakkaSQSWorkerConfig) {
      unpackBagRequest: UnpackBagRequest =>
        val location =
          BagLocationBuilder.build(unpackBagRequest, bagUnpackerWorkerConfig)
        for {
          summaryResult <- unpacker.unpack(
            unpackBagRequest.requestId.toString,
            unpackBagRequest.sourceLocation,
            location.objectLocation)
          stepResult = stepResultFor(summaryResult)
          _ <- ingestUpdater.send(unpackBagRequest.requestId, stepResult)
          _ <- outgoingPublisher.sendIfSuccessful(
            stepResult,
            BagRequest(unpackBagRequest.requestId, location))
        } yield
          stepResult match {
            case IngestStepSucceeded(s) => Successful(Some(s))
            case IngestCompleted(s)     => Successful(Some(s))
            case IngestFailed(s, t, _)  => DeterministicFailure(t, Some(s))
          }
    }

  private def stepResultFor(result: WorkerResult[UnpackSummary]) = {
    result match {
      case workSucceeded: WorkerSucceeded[UnpackSummary] =>
        IngestStepSucceeded(workSucceeded.summary)
      case WorkerFailed(
          unpackSummary: UnpackSummary,
          archiveLocationException: ArchiveLocationException) =>
        IngestFailed(
          unpackSummary,
          archiveLocationException,
          Some(clientMessageFor(archiveLocationException)))
      case WorkerFailed(s: UnpackSummary, e: Throwable) =>
        IngestFailed(s, e)
    }
  }

  private def clientMessageFor(exception: ArchiveLocationException) = {
    val cause = exception.getCause.asInstanceOf[AmazonS3Exception]
    val archiveLocation = exception.getObjectLocation
    cause.getStatusCode match {
      case 403 => s"access to $archiveLocation is denied"
      case 400 => s"$archiveLocation is invalid"
      case 404 => s"$archiveLocation does not exist"
      case _   => s"$archiveLocation could not be downloaded"
    }
  }

  override def run(): Future[Any] = worker.start

}
