package uk.ac.wellcome.platform.archive.indexer.bags

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{Indexer, IndexerWorker}

import scala.concurrent.Future

class BagIndexerWorker(
                        val config: AlpakkaSQSWorkerConfig,
                        val indexer: Indexer[StorageManifest, IndexedStorageManifest],
                        val metricsNamespace: String
                      )(
                        implicit
                        val actorSystem: ActorSystem,
                        val sqsAsync: SqsAsyncClient,
                        val monitoringClient: MetricsMonitoringClient,
                        val decoder: Decoder[StorageManifest]
                      ) extends IndexerWorker[StorageManifest, StorageManifest, IndexedStorageManifest](config, indexer, metricsNamespace) {

  def load(source: StorageManifest): Future[StorageManifest] = Future.successful(source)
}

