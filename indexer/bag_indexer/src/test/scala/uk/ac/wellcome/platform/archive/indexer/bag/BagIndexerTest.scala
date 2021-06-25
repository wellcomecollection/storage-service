package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.{ElasticClient, Index}
import org.scalatest.Assertion
import weco.storage.generators.StorageManifestGenerators
import weco.storage_service.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.{
  BagIndexer,
  BagsIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  StorageServiceIndexConfig
}
import weco.json.JsonUtil._
import weco.storage_service.bagit.models.BagVersion

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
