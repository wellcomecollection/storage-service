package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.get.GetResponse
import com.sksamuel.elastic4s.{Index, Response}
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_indexer.fixtures.ElasticsearchFixtures
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerTest extends FunSpec with Matchers with ScalaFutures with Eventually with IntegrationPatience with ElasticsearchFixtures with StorageManifestGenerators {
  it("indexes a single manifest") {
    val manifest = createStorageManifest

    withIndexes { case (manifestsIndex, filesIndex) =>
      val future =
        withBagIndexer(manifestsIndex, filesIndex) {
          _.index(manifest)
        }

      whenReady(future) { _ =>
        eventually {
          val response: Response[GetResponse] = elasticClient.execute {
            get(s"${manifest.id}/${manifest.version}").from(manifestsIndex)
          }.await

          val getResponse = response.result

          getResponse.exists shouldBe true

          fromJson[StorageManifest](getResponse.sourceAsString).get shouldBe manifest
        }
      }
    }
  }

  private def withBagIndexer[R](manifestsIndex: Index, filesIndex: Index)(testWith: TestWith[BagIndexer, R]): R =
    testWith(
      new BagIndexer(
        elasticClient = elasticClient,
        manifestsIndex = manifestsIndex,
        filesIndex = filesIndex
      )
    )
}
