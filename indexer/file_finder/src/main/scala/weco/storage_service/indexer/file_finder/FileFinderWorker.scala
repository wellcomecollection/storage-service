package weco.storage_service.indexer.file_finder

import org.apache.pekko.actor.ActorSystem
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.messaging.sqsworker.pekko.{
  PekkoSQSWorker,
  PekkoSQSWorkerConfig
}
import weco.messaging.worker.models.{Result, RetryableFailure, Successful}
import weco.monitoring.Metrics
import weco.storage_service.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerUnknownGetError
}
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.indexer.models.FileContext
import weco.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class FileFinderWorker(
  val config: PekkoSQSWorkerConfig,
  val bagTrackerClient: BagTrackerClient,
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
  mc: Metrics[Future],
  wd: Decoder[BagRegistrationNotification],
  ec: ExecutionContext
) extends Runnable
    with Logging {

  private val worker =
    new PekkoSQSWorker[BagRegistrationNotification, Nothing](config)(
      processMessage
    )

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
            Left(RetryableFailure(e))
          case Left(e) =>
            error(new Exception(s"Failed to load $notification, got $e"))
            Left(
              RetryableFailure(
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
          .recover { case t: Throwable => RetryableFailure(t) }

      case Left(err) => Future.successful(err)
    }
  }

  override def run(): Future[Any] = worker.start
}
