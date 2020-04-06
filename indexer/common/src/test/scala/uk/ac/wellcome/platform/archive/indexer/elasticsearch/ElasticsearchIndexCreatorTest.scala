package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.indexes.IndexResponse
import com.sksamuel.elastic4s.{RequestFailure, Response}
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchIndexCreatorTest
    extends FunSpec
    with ElasticsearchFixtures
    with Matchers {

  it("allows you to index an object that matches the mapping") {
    val nameMapping = properties(
      Seq(textField("name"))
    )

    withLocalElasticsearchIndex(nameMapping) { index =>
      val indexFuture =
        elasticClient
          .execute {
            indexInto(index).doc("""{"name": "James Stagg"}""")
          }

      whenReady(indexFuture) { indexResponse =>
        assertIsSuccess(indexResponse)

        eventually {
          val results = searchT[Map[String, String]](
            index = index,
            query = matchAllQuery()
          )

          results shouldBe Seq(Map("name" -> "James Stagg"))
        }
      }
    }
  }

  it("stops you from indexing an object that doesn't match the mapping") {
    val nameMapping = properties(
      Seq(textField("name"))
    )

    withLocalElasticsearchIndex(nameMapping) { index =>
      val indexFuture =
        elasticClient
          .execute {
            indexInto(index).doc("""{"profession": "meterologist"}""")
          }

      whenReady(indexFuture) { response =>
        response.isError shouldBe true
        response shouldBe a[RequestFailure]
      }
    }
  }

  it("updates an index with a compatible mapping") {
    val placeMapping = properties(
      Seq(textField("name"))
    )

    val placeWithCountryMapping = properties(
      Seq(textField("name"), textField("country"))
    )

    withLocalElasticsearchIndex(placeMapping) { index =>
      withLocalElasticsearchIndex(placeWithCountryMapping, index = index) {
        modifiedIndex =>
          val indexFuture =
            elasticClient
              .execute {
                indexInto(modifiedIndex)
                  .doc(
                    """
                    |{
                    |  "name": "Blacksod Point",
                    |  "country": "Republic of Ireland"
                    |}""".stripMargin
                  )
              }

          whenReady(indexFuture) { indexResponse: Response[IndexResponse] =>
            assertIsSuccess(indexResponse)

            eventually {
              val results = searchT[Map[String, String]](
                index = index,
                query = matchAllQuery()
              )

              results shouldBe Seq(
                Map(
                  "name" -> "Blacksod Point",
                  "country" -> "Republic of Ireland"
                )
              )
            }
          }
      }
    }
  }

  private def assertIsSuccess(
    indexResponse: Response[IndexResponse]
  ): Assertion = {
    if (indexResponse.isError) {
      throw indexResponse.error.asException
    }

    indexResponse.isSuccess shouldBe true
  }
}
