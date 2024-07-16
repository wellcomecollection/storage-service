package weco.storage_service.notifier.services

import org.apache.pekko.actor.ActorSystem
import grizzled.slf4j.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.messaging.sqsworker.pekko.{
  PekkoSQSWorker,
  PekkoSQSWorkerConfig
}
import weco.messaging.worker.models.{Result, Successful}
import weco.monitoring.Metrics
import weco.storage_service.ingests.models.{
  CallbackNotification,
  IngestCallbackStatusUpdate,
  IngestUpdate
}
import weco.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorker[Destination](
  config: PekkoSQSWorkerConfig,
  callbackUrlService: CallbackUrlService,
  messageSender: MessageSender[Destination]
)(
  implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: Metrics[Future],
  sc: SqsAsyncClient
) extends Runnable
    with Logging {
  private val worker =
    new PekkoSQSWorker[CallbackNotification, IngestCallbackStatusUpdate](
      config
    )(processMessage)

  def processMessage(
    callbackNotification: CallbackNotification
  ): Future[Result[IngestCallbackStatusUpdate]] =
    for {
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
    } yield Successful(summary = Some(ingestUpdate))

  override def run(): Future[Any] = worker.start
}
