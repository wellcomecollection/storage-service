package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.get.GetResponse
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{Index, Response}
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_indexer.fixtures.ElasticsearchFixtures
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageManifestFile}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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

  it("fails if something goes wrong with indexing") {
    withIndexes { case (manifestsIndex, filesIndex) =>

      // Get the index names wrong, so the mappings will reject the manifest.
      val future =
        withBagIndexer(filesIndex, manifestsIndex) {
          _.index(createStorageManifest)
        }

      whenReady(future.failed) {
        _ shouldBe a[RuntimeException]
      }
    }
  }

  it("we can query the bags on the properties of files") {
    val manifest1 = createStorageManifestWith(
      manifestFiles = Seq(
        StorageManifestFile(
          checksum = randomChecksumValue,
          name = "alice.txt",
          path = "v1/alice.txt",
          size = 100
        )
      )
    )

    val manifest2 = createStorageManifestWith(
      manifestFiles = Seq(
        StorageManifestFile(
          checksum = randomChecksumValue,
          name = "alice.txt",
          path = "v1/alice.txt",
          size = 200
        ),
        StorageManifestFile(
          checksum = randomChecksumValue,
          name = "bob.txt",
          path = "v1/bob.txt",
          size = 100
        )
      )
    )

    withIndexes { case (manifestsIndex, filesIndex) =>
      val futures =
        Seq(manifest1, manifest2).map { manifest =>
          withBagIndexer(manifestsIndex, filesIndex) {
            _.index(manifest)
          }
        }

      whenReady(Future.sequence(futures)) { _ =>
        eventually {
          val response: Response[SearchResponse] = elasticClient.execute {
            search(manifestsIndex)
              .query {
                nestedQuery(
                  "manifest.files",
                  must(
                    termQuery("manifest.files.name", "alice.txt"),
                    termQuery("manifest.files.size", 100),
                  )
                )
              }
          }.await

          val searchResponse = response.result
          searchResponse.totalHits shouldBe 1

          searchResponse
            .hits.hits
            .map { hit => fromJson[StorageManifest](hit.sourceAsString).get } shouldBe Seq(manifest1)
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
