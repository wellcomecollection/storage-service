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
  messageSender: MessageSender[_],
  // How this default was derived: the max SNS message size is 256KB.
  // Looking in SQS, the average message size was ~1.42KB, and that's
  // not going to change much: the only variable part of the message is
  // the name/path.  That allows ~180 messages in an SNS payload, with
  // a bit of overhead to be safe.
  batchSize: Int = 140
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
              bag.manifest.files
              // Only index files that are new in this version.
              // e.g. if this is a V2 manifest, skip reindexing files from V1
                .filter { f =>
                  f.path.startsWith(s"${bag.version}/")
                }
                .map { f =>
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
        // Rather than sending individual files, we send bundles of 1000 files
        // at a time.  This means we can make more efficient use of the
        // Elasticsearch bulk API in the file indexer.

        val batches =
          fileContexts
            .grouped(batchSize)
            .toSeq

        val futures =
          batches
            .map { b =>
              Future.fromTry(messageSender.sendT(b))
            }

        Future
          .sequence(futures)
          .map { _ =>
            Successful(None)
          }
          .recover { case t: Throwable => NonDeterministicFailure(t) }

      case Left(err) => Future.successful(err)
    }
  }

  override def run(): Future[Any] = worker.start
}
