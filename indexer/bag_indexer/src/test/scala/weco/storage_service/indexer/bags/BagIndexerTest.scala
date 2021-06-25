package weco.storage_service.indexer.bags

import com.sksamuel.elastic4s.{ElasticClient, Index}
import org.scalatest.Assertion
import weco.storage_service.generators.StorageManifestGenerators
import weco.storage_service.storage.models.StorageManifest
import weco.storage_service.indexer.IndexerTestCases
import weco.storage_service.indexer.bags.models.IndexedStorageManifest
import weco.storage_service.indexer.Indexer
import weco.json.JsonUtil._
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerTest
    extends IndexerTestCases[StorageManifest, IndexedStorageManifest]
    with StorageManifestGenerators {

  val indexConfig: StorageServiceIndexConfig = BagsIndexConfig

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
