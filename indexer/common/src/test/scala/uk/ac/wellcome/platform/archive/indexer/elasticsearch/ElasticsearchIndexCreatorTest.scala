package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{Indexable, RequestFailure}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

case class TestObject(id: String, description: String, visible: Boolean)

class ElasticsearchIndexCreatorTest
  extends FunSpec
    with ElasticsearchFixtures
    with ScalaFutures
    with Eventually
    with Matchers
    with JsonAssertions
    with BeforeAndAfterEach {

  case class Shape(name: String, sides: Int)
  case class Crate(weight: Int)

  implicit object ShapeIndexable extends Indexable[Shape] {
    override def json(shape: Shape): String =
      toJson(shape).get
  }

  implicit object CrateIndexable extends Indexable[Crate] {
    override def json(crate: Crate): String =
      toJson(crate).get
  }

  val shapeMapping: MappingDefinition = properties(
    Seq(
      textField("name"),
    )
  )

  val shapeMappingWithCount: MappingDefinition = properties(
    Seq(
      textField("name"),
      intField("sides")
    )
  )

  it("allows you to index an object that matches the mapping") {
    val square = Shape(name = "square", sides = 4)

    withLocalElasticsearchIndex(shapeMapping) { index =>
      val indexFuture =
        elasticClient
          .execute {
            indexInto(index).doc(square)
          }

      whenReady(indexFuture) { indexResponse =>
        indexResponse.isSuccess shouldBe true

        val results = searchT[Shape](index = index, query = matchAllQuery())

        results shouldBe Seq(square)
      }
    }
  }

  it("stops you from indexing an object that doesn't match the mapping") {
    val crate = Crate(weight = 5)

    withLocalElasticsearchIndex(shapeMapping) { index =>
      val indexFuture =
        elasticClient
          .execute {
            indexInto(index).doc(crate)
          }

      whenReady(indexFuture) { response =>
        response.isError shouldBe true
        response shouldBe a[RequestFailure]
      }
    }
  }

  it("updates an index with a compatible mapping") {
    val triangle = Shape(name = "triangle", sides = 3)

    withLocalElasticsearchIndex(shapeMapping) { index =>
      withLocalElasticsearchIndex(shapeMappingWithCount, index = index) { modifiedIndex =>
        val indexFuture =
          elasticClient
            .execute {
              indexInto(modifiedIndex).doc(triangle)
            }

        whenReady(indexFuture) { indexResponse =>
          indexResponse.isSuccess shouldBe true

          val results = searchT[Shape](index = index, query = matchAllQuery())

          results shouldBe Seq(triangle)
        }
      }
    }
  }
}
