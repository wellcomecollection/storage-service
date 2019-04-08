package uk.ac.wellcome.platform.archive.notifier.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  DeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  IngestCallbackStatusUpdate,
  IngestUpdate
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  callbackUrlService: CallbackUrlService,
  snsWriter: SNSWriter
)(implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging {
  private val worker =
    AlpakkaSQSWorker[CallbackNotification, IngestCallbackStatusUpdate](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(callbackNotification: CallbackNotification)
    : Future[Result[IngestCallbackStatusUpdate]] = {
    val future = for {
      httpResponse <- callbackUrlService.getHttpResponse(callbackNotification)
      ingestUpdate = PrepareNotificationService.prepare(
        callbackNotification.id,
        httpResponse)

      _ <- snsWriter.writeMessage[IngestUpdate](
        ingestUpdate,
        subject = s"Sent by ${this.getClass.getName}"
      )
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
