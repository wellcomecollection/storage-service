package uk.ac.wellcome.platform.archive.bag_indexer.fixtures

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.requests.indexes.admin.IndexExistsResponse
import com.sksamuel.elastic4s.{ElasticClient, Index, Response}
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Assertion, Matchers, Suite}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

trait ElasticsearchFixtures
    extends Eventually
    with ScalaFutures
    with Matchers
    with JsonAssertions
    with IntegrationPatience { this: Suite =>

  private val esHost = "localhost"
  private val esPort = 9200

  val elasticClient: ElasticClient = ElasticClientBuilder.create(
    hostname = esHost,
    port = esPort,
    protocol = "http",
    username = "elastic",
    password = "changeme"
  )

  // Elasticsearch takes a while to start up so check that it actually started
  // before running tests.
  eventually {
    val response: Response[ClusterHealthResponse] = elasticClient
      .execute(clusterHealth())
      .await

    response.result.numberOfNodes shouldBe 1
  }(
    PatienceConfig(
      timeout = scaled(Span(40, Seconds)),
      interval = scaled(Span(150, Millis))
    ),
    implicitly[Position])

  private val elasticsearchIndexCreator = new ElasticsearchIndexCreator(
    elasticClient = elasticClient
  )

  def withLocalElasticsearchIndex[R](
    config: IndexConfig,
    index: Index = createIndex): Fixture[Index, R] = fixture[Index, R](
    create = {
      elasticsearchIndexCreator
        .create(index = index, config = config)
        .await

      // Elasticsearch is eventually consistent, so the future
      // completing doesn't actually mean that the index exists yet
      eventuallyIndexExists(index)

      index
    },
    destroy = { index =>
      elasticClient.execute(deleteIndex(index.name))
    }
  )

  def withIndexes[R](testWith: TestWith[(Index, Index), R]): R =
    withLocalElasticsearchIndex(config = ManifestIndexConfig) { manifestsIndex =>
      withLocalElasticsearchIndex(config = FilesIndexConfig) { filesIndex =>
        testWith((manifestsIndex, filesIndex))
      }
    }

  def eventuallyIndexExists(index: Index): Assertion =
    eventually {
      val response: Response[IndexExistsResponse] =
        elasticClient
          .execute(indexExists(index.name))
          .await

      response.result.isExists shouldBe true
    }

  private def createIndex: Index =
    Index(name = (Random.alphanumeric take 10 mkString) toLowerCase)
}
