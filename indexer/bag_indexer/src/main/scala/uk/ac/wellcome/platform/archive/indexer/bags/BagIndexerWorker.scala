package uk.ac.wellcome.platform.archive.indexer.bags

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_tracker.client.{BagTrackerClient, BagTrackerGetError, BagTrackerUnknownGetError}
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{FatalIndexingError, Indexer, IndexerWorker, IndexerWorkerError, RetryableIndexingError}
import uk.ac.wellcome.storage._

import scala.concurrent.Future

class BagIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[StorageManifest, IndexedStorageManifest],
  val bagTrackerClient: BagTrackerClient,
  val metricsNamespace: String,
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val monitoringClient: MetricsMonitoringClient,
  val decoder: Decoder[KnownReplicasPayload]
) extends IndexerWorker[
      KnownReplicasPayload,
      StorageManifest,
      IndexedStorageManifest
    ](config, indexer, metricsNamespace) {

  def load(
    source: KnownReplicasPayload
  ): Future[Either[IndexerWorkerError, StorageManifest]] =
      bagTrackerClient.getBag(
        bagId = BagId(
          space = source.storageSpace,
          externalIdentifier = source.externalIdentifier
        ),
        version = source.version
      ) map {
      case Right(manifest) => Right(manifest)
      case Left(BagTrackerUnknownGetError(e)) =>
        warn(f"BagTrackerUnknownGetError: Failed to load $source, got $e")
        Left(
          RetryableIndexingError(
            payload = source,
            cause = e
          )
        )
      case Left(e) =>
        error(new Exception(f"Failed to load $source, got $e"))
        Left(FatalIndexingError(payload = source))
    }
}
