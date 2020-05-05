package uk.ac.wellcome.platform.storage.ingests_worker.services

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{
  MetricsMonitoringClient,
  MetricsMonitoringProcessor
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class IngestsWorker(
  workerConfig: AlpakkaSQSWorkerConfig,
  metricsNamespace: String,
  trackerHost: Uri
)(
  implicit actorSystem: ActorSystem,
  mc: MetricsMonitoringClient,
  sc: SqsAsyncClient
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  private val monitoringProcessorBuilder = (ec: ExecutionContext) =>
    new MetricsMonitoringProcessor[IngestUpdate](metricsNamespace)(mc, ec)

  private val worker =
    AlpakkaSQSWorker[IngestUpdate, Instant, Instant, Ingest](
      config = workerConfig,
      monitoringProcessorBuilder = monitoringProcessorBuilder
    )(processMessage)

  def processMessage(ingestUpdate: IngestUpdate): Future[Result[Ingest]] = {
    val path = Path(f"ingest/${ingestUpdate.id}")
    val requestUri = trackerHost.withPath(path)

    val updatedIngest = for {
      ingestUpdateEntity <- Marshal(ingestUpdate).to[RequestEntity]

      request = HttpRequest(
        uri = requestUri,
        method = HttpMethods.PATCH,
        entity = ingestUpdateEntity
      )

      _ = info(f"Making request: $request")

      response <- Http().singleRequest(request)

      ingest <- response.status match {
        case StatusCodes.OK =>
          info(f"Got OK for PATCH to $requestUri with $ingestUpdate")
          Unmarshal(response.entity).to[Ingest]
        case status =>
          val err = new Exception(f"$status from IngestsTracker")
          error(f"NOT OK for PATCH to $requestUri with $ingestUpdate", err)
          Future.failed(err)
      }
    } yield ingest

    updatedIngest
      .map { ingest =>
        info(f"Successfully sent $ingestUpdate, got $ingest")
        Successful(Some(ingest))
      }
      .recover {
        case err =>
          warn(s"Error trying to apply update $ingestUpdate: $err")
          NonDeterministicFailure(err, summary = None)
      }
  }

  override def run(): Future[Any] = worker.start
}
