package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, Index}
import org.scalatest.Assertion
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.{BagIndexer, BagsIndexConfig}
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerTest
    extends IndexerTestCases[StorageManifest, IndexedStorageManifest]
    with StorageManifestGenerators {

  override val mapping: MappingDefinition = BagsIndexConfig.mapping

  override def createIndexer(
    client: ElasticClient,
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(client, index)

  override def createDocument: StorageManifest = createStorageManifest

  override def id(document: StorageManifest): String = document.id.toString

  override def assertMatch(
    indexedDocument: IndexedStorageManifest,
    expectedDocument: StorageManifest
  ): Assertion =
    IndexedStorageManifest(expectedDocument) shouldBe indexedDocument

  override def getDocument(index: Index, id: String): IndexedStorageManifest =
    getT[IndexedStorageManifest](index, id)

  override def createDocumentPair: (StorageManifest, StorageManifest) = {
    val storageManifest = createStorageManifest

    val olderStorageManifest = createStorageManifestWith(
      ingestId = storageManifest.ingestId,
      version = BagVersion(1)
    )
    val newerStorageManifest = olderStorageManifest.copy(
      ingestId = storageManifest.ingestId,
      version = BagVersion(2)
    )

    assert(
      olderStorageManifest.version.underlying < newerStorageManifest.version.underlying
    )

    (olderStorageManifest, newerStorageManifest)
  }
}
