package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
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
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.exception.{
  InvalidObjectLocationException,
  PutObjectLocationException
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagRequest,
  UnpackBagRequest
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.models.{
  WorkFailed,
  WorkResult,
  WorkSucceeded
}
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepSuccess
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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
          summaryResult <- stepResultFor(
            unpacker.unpack(
              unpackBagRequest.requestId.toString,
              unpackBagRequest.sourceLocation,
              location.objectLocation))
          _ <- ingestUpdater.send(unpackBagRequest.requestId, summaryResult)
          _ <- outgoingPublisher.sendIfSuccessful(
            summaryResult,
            BagRequest(unpackBagRequest.requestId, location))

          result = summaryResult match {
            case IngestStepSuccess(s)  => Successful(Some(s))
            case IngestCompleted(s)    => Successful(Some(s))
            case IngestFailed(s, t, _) => DeterministicFailure(t, Some(s))
          }
        } yield result
    }

  private def stepResultFor(
    eventualResult: Future[WorkResult[UnpackSummary]]) = {
    eventualResult.transform {
      case Success(workSucceeded: WorkSucceeded[UnpackSummary]) =>
        Success(IngestStepSuccess(workSucceeded.summary))
      case Success(
          WorkFailed(s: UnpackSummary, e: InvalidObjectLocationException)) =>
        Success(
          IngestFailed(
            s,
            e,
            Some(s"location '${e.getObjectLocation}' was invalid")))
      case Success(
          WorkFailed(s: UnpackSummary, e: PutObjectLocationException)) =>
        Success(
          IngestFailed(s, e, Some(s"${e.getMessage} '${e.getObjectLocation}'")))
      case Success(WorkFailed(s: UnpackSummary, e: Throwable)) =>
        Success(IngestFailed(s, e))
      case Failure(e) =>
        Failure(e)
    }
  }

  override def run(): Future[Any] = worker.start

}
