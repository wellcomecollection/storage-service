package weco.storage_service.indexer.files

import java.time.Instant

import com.sksamuel.elastic4s.{ElasticClient, Index}
import org.scalatest.Assertion
import weco.json.JsonUtil._
import weco.storage_service.indexer.IndexerTestCases
import weco.storage_service.indexer.Indexer
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.files.fixtures.FileIndexerFixtures
import weco.storage_service.indexer.files.models.IndexedFile

import scala.concurrent.ExecutionContext.Implicits.global

class FileIndexerTest
    extends IndexerTestCases[FileContext, IndexedFile]
    with FileIndexerFixtures {
  override def createIndexer(
    client: ElasticClient,
    index: Index
  ): Indexer[FileContext, IndexedFile] =
    new FileIndexer(client = client, index = index)

  override def createDocument: FileContext =
    createContext

  override def id(document: FileContext): String = document.location.toString()

  override def assertMatch(
    indexedDocument: IndexedFile,
    context: FileContext
  ): Assertion =
    IndexedFile(context) shouldBe indexedDocument

  override def getDocument(index: Index, id: String): IndexedFile =
    getT[IndexedFile](index, id = id)

  override def createDocumentPair: (FileContext, FileContext) = {
    val context1 = createDocument.copy(
      createdDate = Instant.parse("2001-01-01T01:01:01Z")
    )

    val context2 = context1.copy(
      createdDate = Instant.parse("2002-02-02T02:02:02Z")
    )

    (context1, context2)
  }
}
