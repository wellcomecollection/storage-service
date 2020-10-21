package uk.ac.wellcome.platform.archive.indexer.file_finder

import java.time.Instant

import akka.actor.ActorSystem
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
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
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerUnknownGetError
}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class FileFinderWorker(
  val config: AlpakkaSQSWorkerConfig,
  val bagTrackerClient: BagTrackerClient,
  val metricsNamespace: String,
  messageSender: MessageSender[_]
)(
  implicit
  actorSystem: ActorSystem,
  sqsAsync: SqsAsyncClient,
  mc: MetricsMonitoringClient,
  wd: Decoder[BagRegistrationNotification],
  ec: ExecutionContext
) extends Runnable
    with Logging {

  private val worker =
    AlpakkaSQSWorker[
      BagRegistrationNotification,
      Instant,
      Instant,
      Nothing
    ](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[BagRegistrationNotification](
          metricsNamespace
        )(mc, ec)
    ) {
      processMessage
    }

  def processMessage(
    notification: BagRegistrationNotification
  ): Future[Result[Nothing]] = {
    debug(s"Processing notification $notification")
    val contexts =
      for {
        version <- Future.fromTry {
          BagVersion.fromString(notification.version)
        }

        bagId = BagId(
          space = notification.space,
          externalIdentifier = notification.externalIdentifier
        )

        _ = debug(s"Fetching bag ID=$bagId version=$version")

        bagLookup <- bagTrackerClient.getBag(bagId = bagId, version = version)

        fileContexts = bagLookup match {
          case Right(bag) =>
            Right(
              bag.manifest.files.map { f =>
                FileContext(bag, f)
              }
            )
          case Left(BagTrackerUnknownGetError(e)) =>
            warn(
              f"BagTrackerUnknownGetError: Failed to load $notification, got $e"
            )
            Left(NonDeterministicFailure(e))
          case Left(e) =>
            error(new Exception(s"Failed to load $notification, got $e"))
            Left(
              NonDeterministicFailure(
                new Throwable(s"Failed to load $notification, got $e")
              )
            )
        }
      } yield fileContexts

    contexts.flatMap {
      case Right(fileContexts) =>
        Future
          .sequence(
            fileContexts.map { c =>
              Future.fromTry(messageSender.sendT(c))
            }
          )
          .map { _ =>
            Successful(None)
          }
          .recover { case t: Throwable => NonDeterministicFailure(t) }

      case Left(err) => Future.successful(err)
    }
  }

  override def run(): Future[Any] = {
    debug(s"@@AWLC run!")
    worker.start
  }
}
