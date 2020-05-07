package uk.ac.wellcome.platform.archive.indexer

import com.sksamuel.elastic4s.ElasticDsl.{matchAllQuery, properties, textField}
import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  ElasticClientFactory,
  Indexer
}
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

trait IndexerTestCases[Document, IndexedDocument]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with ElasticsearchFixtures {
  val mapping: MappingDefinition

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
      withLocalElasticsearchIndex(mapping) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        val document = createDocument

        whenReady(indexer.index(document)) { result =>
          result.right.value shouldBe document

          val indexedDocument: IndexedDocument =
            getDocument(index, id = id(document))

          assertMatch(indexedDocument, document)
        }
      }
    }

    it("indexes multiple documents") {
      withLocalElasticsearchIndex(mapping) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        val documentCount = 10
        val documents = (1 to documentCount).map { _ =>
          createDocument
        }

        whenReady(indexer.index(documents)) { result =>
          result.right.value shouldBe documents

          eventually {
            val storedManifests = searchT[Json](index, query = matchAllQuery())

            storedManifests should have size documentCount
          }
        }
      }
    }

    it("returns a Left if the document can't be indexed correctly") {
      val document = createDocument

      val badMapping = properties(
        Seq(textField("name"))
      )

      withLocalElasticsearchIndex(badMapping) { index =>
        val indexer = createIndexer(elasticClient, index = index)

        whenReady(indexer.index(document)) {
          _.left.value shouldBe document
        }
      }
    }

    it("fails if Elasticsearch doesn't respond") {
      val badClient = ElasticClientFactory.create(
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

        withLocalElasticsearchIndex(mapping) { index =>
          val indexer = createIndexer(elasticClient, index = index)

          val future = indexer
            .index(olderDocument)
            .flatMap { _ =>
              indexer.index(newerDocument)
            }

          whenReady(future) { result =>
            result.right.value shouldBe newerDocument

            val indexedDocument: IndexedDocument =
              getDocument(index, id = id(olderDocument))

            assertMatch(indexedDocument, newerDocument)
          }
        }
      }

      it("an older document replaces an newer document") {
        val (olderDocument, newerDocument) = createDocumentPair

        withLocalElasticsearchIndex(mapping) { index =>
          val indexer = createIndexer(elasticClient, index = index)

          val future = indexer
            .index(newerDocument)
            .flatMap { _ =>
              indexer.index(olderDocument)
            }

          whenReady(future) { result =>
            result.right.value shouldBe olderDocument

            val indexedDocument: IndexedDocument =
              getDocument(index, id = id(olderDocument))

            assertMatch(indexedDocument, newerDocument)
          }
        }
      }
    }
  }
}
