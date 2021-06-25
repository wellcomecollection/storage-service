package weco.storage_service.notifier.services

import java.time.Instant

import akka.actor.ActorSystem
import grizzled.slf4j.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import weco.messaging.worker.models.{
  DeterministicFailure,
  Result,
  Successful
}
import weco.messaging.worker.monitoring.metrics.MetricsMonitoringProcessor
import weco.monitoring.Metrics
import weco.storage_service.ingests.models.{
  CallbackNotification,
  IngestCallbackStatusUpdate,
  IngestUpdate
}
import weco.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorker[Destination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  callbackUrlService: CallbackUrlService,
  messageSender: MessageSender[Destination],
  val metricsNamespace: String
)(
  implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: Metrics[Future],
  sc: SqsAsyncClient
) extends Runnable
    with Logging {
  private val worker =
    AlpakkaSQSWorker[
      CallbackNotification,
      Instant,
      Instant,
      IngestCallbackStatusUpdate
    ](
      alpakkaSQSWorkerConfig,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[CallbackNotification](
          metricsNamespace
        )(mc, ec)
    ) {
      processMessage
    }

  def processMessage(
    callbackNotification: CallbackNotification
  ): Future[Result[IngestCallbackStatusUpdate]] = {
    val future = for {
      httpResponse <- callbackUrlService.getHttpResponse(
        ingest = callbackNotification.payload,
        callbackUri = callbackNotification.callbackUri
      )
      ingestUpdate = PrepareNotificationService.prepare(
        id = callbackNotification.ingestId,
        httpResponse = httpResponse
      )

      _ <- Future.fromTry {
        messageSender.sendT[IngestUpdate](ingestUpdate)
      }
    } yield ingestUpdate

    future
      .map { ingestUpdate =>
        Successful(Some(ingestUpdate))
      }
      .recover {
        case throwable => DeterministicFailure(throwable, summary = None)
      }
  }

  override def run(): Future[Any] = worker.start
}
