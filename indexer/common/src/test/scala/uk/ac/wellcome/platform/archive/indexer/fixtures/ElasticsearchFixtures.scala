package uk.ac.wellcome.platform.archive.indexer.fixtures

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.analysis.Analysis
import com.sksamuel.elastic4s.requests.get.GetResponse
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.requests.searches.queries.Query
import com.sksamuel.elastic4s.{Index, Response}
import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.elasticsearch.IndexConfig
import uk.ac.wellcome.json.JsonUtil.fromJson
import uk.ac.wellcome.elasticsearch.test.fixtures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

trait ElasticsearchFixtures extends fixtures.ElasticsearchFixtures { this: Suite =>

  protected val esHost = "localhost"
  protected val esPort = 9200

  def createIndexConfigWith(m: MappingDefinition): IndexConfig =
    new IndexConfig {
      override def mapping: MappingDefinition = m.dynamic(DynamicMapping.Strict)

      override def analysis: Analysis = Analysis(analyzers = List())
    }

  protected def getT[T](index: Index, id: String)(
    implicit decoder: Decoder[T]
  ): T = {
    val response: Response[GetResponse] =
      elasticClient.execute { get(index, id) }.await

    val getResponse = response.result
    getResponse.exists shouldBe true

    fromJson[T](getResponse.sourceAsString) match {
      case Success(t) => t
      case Failure(err) =>
        throw new Throwable(
          s"Unable to parse source string ($err): ${getResponse.sourceAsString}"
        )
    }
  }

  protected def searchT[T](index: Index, query: Query)(
    implicit decoder: Decoder[T]
  ): Seq[T] = {
    val response: Response[SearchResponse] =
      elasticClient.execute { search(index).query(query) }.await

    response.result.hits.hits
      .map { hit =>
        fromJson[T](hit.sourceAsString).get
      }
  }
}
