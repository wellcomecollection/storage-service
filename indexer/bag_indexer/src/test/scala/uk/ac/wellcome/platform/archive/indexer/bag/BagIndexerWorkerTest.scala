package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.{PayloadGenerators, StorageManifestGenerators}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.{BagIndexer, BagIndexerWorker, BagsIndexConfig}
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{Indexer, IndexerWorker}

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerWorkerTest
    extends IndexerWorkerTestCases[
      KnownReplicasPayload,
      StorageManifest,
      IndexedStorageManifest
    ]
      with StorageManifestGenerators
      with PayloadGenerators
      with StorageManifestVHSFixture {

  override val mapping: MappingDefinition = BagsIndexConfig.mapping

  override def createT: (KnownReplicasPayload, String) = {
    val payload = createKnownReplicasPayload

    val bagId = BagId(
      space = payload.storageSpace,
      externalIdentifier = payload.externalIdentifier
    )

    (payload, bagId.toString)
  }

  def createIndexer(
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(
      client = elasticClient,
      index = index
    )

  override def convertToIndexed(
    manifest: KnownReplicasPayload
  ): IndexedStorageManifest = {
    // TODO: make this actually work
    val storageManifest = createStorageManifest

    IndexedStorageManifest(storageManifest)
  }

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
