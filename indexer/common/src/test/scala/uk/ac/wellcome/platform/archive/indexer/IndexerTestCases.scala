package uk.ac.wellcome.platform.archive.indexer

import com.sksamuel.elastic4s.ElasticDsl.{matchAllQuery, properties, textField}
import com.sksamuel.elastic4s.analysis.Analysis
import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import uk.ac.wellcome.elasticsearch.{ElasticClientBuilder, IndexConfig}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  StorageServiceIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

trait IndexerTestCases[Document, IndexedDocument]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with ElasticsearchFixtures {

  val indexConfig: StorageServiceIndexConfig

  def createIndexer(
    client: ElasticClient,
    index: Index
  ): Indexer[Document, IndexedDocument]

  def createDocument: Document
  def id(document: Document): String

  def assertMatch(
    indexedDocument: IndexedDocument,
    expectedDocument: Document
  ): Assertion

  def getDocument(index: Index, id: String): IndexedDocument

  // Create a pair of documents: one older, one newer
  def createDocumentPair: (Document, Document)

  describe("it behaves as an Indexer") {
    it("indexes a single document") {
      withLocalElasticsearchIndex(indexConfig) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        val document = createDocument

        whenReady(indexer.index(document)) { result =>
          result.value shouldBe document

          val indexedDocument: IndexedDocument =
            getDocument(index, id = id(document))

          assertMatch(indexedDocument, document)
        }
      }
    }

    it("indexes multiple documents") {
      withLocalElasticsearchIndex(indexConfig) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        val documentCount = 10
        val documents = (1 to documentCount).map { _ =>
          createDocument
        }

        whenReady(indexer.index(documents)) { result =>
          result.value shouldBe documents

          eventually {
            val storedManifests = searchT[Json](index, query = matchAllQuery())

            storedManifests should have size documentCount
          }
        }
      }
    }

    it("returns a Left if the document can't be indexed correctly") {
      val document = createDocument

      val badConfig = IndexConfig(
        mapping =
          properties(Seq(textField("name")))
            .dynamic(DynamicMapping.Strict),
        analysis = Analysis(analyzers = List())
      )

      withLocalElasticsearchIndex(badConfig) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        whenReady(indexer.index(document)) {
          _.left.value shouldBe document
        }
      }
    }

    it("fails if Elasticsearch doesn't respond") {
      val badClient = ElasticClientBuilder.create(
        hostname = esHost,
        port = esPort + 1,
        protocol = "http",
        username = "elastic",
        password = "changeme"
      )

      val index = createIndex
      val indexer = createIndexer(badClient, index = index)

      val document = createDocument

      whenReady(indexer.index(document).failed) {
        _ shouldBe a[JavaClientExceptionWrapper]
      }
    }

    describe("orders updates correctly") {
      it("a newer document replaces an older document") {
        val (olderDocument, newerDocument) = createDocumentPair

        withLocalElasticsearchIndex(indexConfig) { index =>
          val indexer = createIndexer(elasticClient, index = index)

          val future = indexer
            .index(olderDocument)
            .flatMap { _ =>
              indexer.index(newerDocument)
            }

          whenReady(future) { result =>
            result.value shouldBe newerDocument

            val indexedDocument: IndexedDocument =
              getDocument(index, id = id(olderDocument))

            assertMatch(indexedDocument, newerDocument)
          }
        }
      }

      it("an older document replaces an newer document") {
        val (olderDocument, newerDocument) = createDocumentPair

        withLocalElasticsearchIndex(indexConfig) { index =>
          val indexer = createIndexer(elasticClient, index = index)

          val future = indexer
            .index(newerDocument)
            .flatMap { _ =>
              indexer.index(olderDocument)
            }

          whenReady(future) { result =>
            result.value shouldBe olderDocument

            val indexedDocument: IndexedDocument =
              getDocument(index, id = id(olderDocument))

            assertMatch(indexedDocument, newerDocument)
          }
        }
      }
    }
  }
}
