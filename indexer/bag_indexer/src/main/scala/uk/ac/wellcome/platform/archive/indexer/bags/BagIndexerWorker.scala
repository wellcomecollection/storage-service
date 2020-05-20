package uk.ac.wellcome.platform.archive.indexer.bags

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{Indexer, IndexerWorker}

import scala.concurrent.Future

class BagIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[StorageManifest, IndexedStorageManifest],
  val metricsNamespace: String,
  storageManifestDao: StorageManifestDao
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

  def load(source: KnownReplicasPayload): Future[StorageManifest] = {

    val bagId = BagId(
      space = source.storageSpace,
      externalIdentifier = source.externalIdentifier
    )

    val bagVersion = source.version

    // TODO:
    //  - actually handle errors
    //  - fix either/future weirdness
    val storageManifest = Future {
      storageManifestDao.get(bagId, bagVersion) match  {
        case Left(_) => throw new Exception("OH GAWD")
        case Right(storageManifest) => storageManifest
      }
    }

    storageManifest
  }
}
