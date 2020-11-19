package uk.ac.wellcome.platform.archive.indexer.bags

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerNotFoundError,
  BagTrackerUnknownGetError
}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  FatalIndexingError,
  Indexer,
  IndexerWorker,
  IndexerWorkerError,
  RetryableIndexingError
}

import scala.concurrent.Future

class BagIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[StorageManifest, IndexedStorageManifest],
  val bagTrackerClient: BagTrackerClient,
  val metricsNamespace: String
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val metrics: Metrics[Future],
  val decoder: Decoder[BagRegistrationNotification]
) extends IndexerWorker[
      BagRegistrationNotification,
      StorageManifest,
      IndexedStorageManifest
    ](config, indexer, metricsNamespace) {

  def load(
    notification: BagRegistrationNotification
  ): Future[Either[IndexerWorkerError, StorageManifest]] =
    for {
      version <- Future.fromTry {
        BagVersion.fromString(notification.version)
      }

      bagId = BagId(
        space = notification.space,
        externalIdentifier = notification.externalIdentifier
      )

      bagLookup <- bagTrackerClient.getBag(bagId = bagId, version = version)

      result = bagLookup match {
        case Right(bag) => Right(bag)
        case Left(BagTrackerUnknownGetError(e)) =>
          warn(
            f"BagTrackerUnknownGetError: Failed to load $notification, got $e"
          )
          Left(
            RetryableIndexingError(
              payload = notification,
              cause = e
            )
          )

        // It's unusual for the bag indexer to be asked to index a non-existent
        // bag.  Since the cost of retrying is small and this might be a consistency
        // issue, allow retrying the bag in a few minutes.
        // See https://github.com/wellcomecollection/platform/issues/4873
        case Left(BagTrackerNotFoundError()) =>
          val message =
            s"Bag indexer asked to index $notification, but the bag tracker doesn't know about that"
          warn(message)
          Left(
            RetryableIndexingError(
              payload = notification,
              cause = new Throwable(message)
            )
          )

        case Left(e) =>
          error(new Exception(f"Failed to load $notification, got $e"))
          Left(FatalIndexingError(payload = notification))
      }
    } yield result
}
