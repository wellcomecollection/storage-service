package weco.storage_service.bag_tagger.services

import java.time.Instant
import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import weco.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import weco.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import weco.monitoring.Metrics
import weco.storage_service.bag_tracker.client.BagTrackerClient
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.storage.models.{
  StorageManifest,
  StorageManifestFile
}
import weco.typesafe.Runnable

import scala.concurrent.Future

class BagTaggerWorker(
  config: AlpakkaSQSWorkerConfig,
  bagTrackerClient: BagTrackerClient,
  applyTags: ApplyTags,
  tagRules: StorageManifest => Map[StorageManifestFile, Map[String, String]]
)(
  implicit
  val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[BagRegistrationNotification]
) extends Runnable
    with Logging {

  implicit val ec = as.dispatcher

  def process(
    notification: BagRegistrationNotification
  ): Future[Result[Unit]] = {
    val result: Future[Result[Unit]] = for {
      version <- Future.fromTry {
        BagVersion.fromString(notification.version)
      }

      bagId = BagId(
        space = notification.space,
        externalIdentifier = notification.externalIdentifier
      )

      manifest <- bagTrackerClient
        .getBag(bagId = bagId, version = version)
        .map {
          case Right(bag) => bag
          case Left(err) =>
            throw new Throwable(
              s"Unable to get bag $bagId version $version from the tracker: $err"
            )
        }

      tagsToApply = tagRules(manifest)

      _ <- Future.fromTry {
        applyTags.applyTags(
          storageLocations = Seq(manifest.location) ++ manifest.replicaLocations,
          tagsToApply = tagsToApply
        )
      }

      _ = info(s"Successfully applied tags for $notification")
      result = Successful[Unit]()
    } yield result

    // We can't be sure what the error is here.  The cost of retrying it is
    // very cheap, so assume it's a flaky error and can be retried.
    result
      .recover {
        case err: Throwable =>
          NonDeterministicFailure[Unit](err)
      }
  }

  val worker
    : AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit](config)(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(message, visibilityTimeout = 0)
    }

  override def run(): Future[Any] = worker.start
}
