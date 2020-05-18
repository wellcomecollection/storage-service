package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.{
  BagIndexer,
  BagsIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerFeatureTest
    extends IndexerFeatureTestCases[StorageManifest, IndexedStorageManifest]
    with StorageManifestGenerators {

  override def convertIndexedT(t: StorageManifest): IndexedStorageManifest =
    IndexedStorageManifest(t)

  def createT: (StorageManifest, String) = {
    val storageManifest = createStorageManifest

    (storageManifest, storageManifest.id.toString)
  }

  override def createIndexer(
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(
      client = elasticClient,
      index = index
    )

  override val mapping: MappingDefinition = BagsIndexConfig.mapping
}
