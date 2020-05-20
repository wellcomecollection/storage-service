package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.{BagIndexer, BagIndexerWorker, BagsIndexConfig}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{Indexer, IndexerWorker}

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerFeatureTest
    extends IndexerFeatureTestCases[
      KnownReplicasPayload,
      StorageManifest,
      IndexedStorageManifest
    ]
      with StorageManifestGenerators
      with StorageManifestVHSFixture {

  override def convertIndexedT(
    manifest: StorageManifest
  ): IndexedStorageManifest =
    IndexedStorageManifest(manifest)

  def createT: (StorageManifest, String) = {
    val storageManifest = createStorageManifest

    (storageManifest, storageManifest.id.toString)
  }

  def createIndexer(
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(
      client = elasticClient,
      index = index
    )

  override val mapping: MappingDefinition = BagsIndexConfig.mapping

  override def withIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[
      IndexerWorker[KnownReplicasPayload, StorageManifest, IndexedStorageManifest],
      R
    ]
  )(implicit decoder: Decoder[KnownReplicasPayload]): R = {
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val storageManifestDao = createStorageManifestDao()

        val worker = new BagIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          metricsNamespace = "indexer",
          storageManifestDao = storageManifestDao
        )

        testWith(worker)
      }
    }
  }
}
