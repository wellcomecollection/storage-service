package uk.ac.wellcome.platform.archive.indexer.files

import java.time.Instant

import com.sksamuel.elastic4s.{ElasticClient, Index}
import org.scalatest.Assertion
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.indexer.IndexerTestCases
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.files.fixtures.FileIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.files.models.IndexedFile

import scala.concurrent.ExecutionContext.Implicits.global

class FileIndexerTest extends IndexerTestCases[FileContext, IndexedFile] with FileIndexerFixtures {
  override def createIndexer(client: ElasticClient, index: Index): Indexer[FileContext, IndexedFile] =
    new FileIndexer(client = client, index = index)

  override def createDocument: FileContext = {
    val (context, _) = createT
    context
  }

  override def id(document: FileContext): String = document.location.toString()

  override def assertMatch(indexedDocument: IndexedFile, context: FileContext): Assertion =
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
