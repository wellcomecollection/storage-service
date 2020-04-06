package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Index
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_indexer.fixtures.ElasticsearchFixtures
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagIndexerTest extends FunSpec with Matchers with ScalaFutures with Eventually with IntegrationPatience with ElasticsearchFixtures with StorageManifestGenerators {
  describe("indexing storage manifests") {
    it("indexes a single manifest") {
      val manifest = createStorageManifest

      withIndexes { case (manifestsIndex, filesIndex) =>
        val future =
          withBagIndexer(manifestsIndex, filesIndex) {
            _.index(manifest)
          }

        whenReady(future) { _ =>
          getT[StorageManifest](manifestsIndex, id = manifest.idWithVersion) shouldBe manifest
        }
      }
    }

    it("fails if something goes wrong with indexing") {
      withIndexes { case (manifestsIndex, filesIndex) =>

        // Get the index names wrong, so the mappings will reject the manifest.
        val future =
          withBagIndexer(manifestsIndex = filesIndex, filesIndex = manifestsIndex) {
            _.index(createStorageManifest)
          }

        whenReady(future.failed) {
          _ shouldBe a[RuntimeException]
        }
      }
    }

    it("we can query the bags on the properties of files") {
      // This test is pretty much the canonical example for nested documents in Elasticsearch.
      //
      // Manifest 1 has file  {alice.txt, size = 100}
      // Manifest 2 has files {alice.txt, size = 200}
      //                      {bob.txt,   size = 100}
      //
      // When we query bags by attributes on files, we want them to match on a per-file level.
      // That is, (name = alice.txt AND size = 100) should only match manifest 1, even though
      // manifest 2 has a file with (name = alice.txt) and a file with (size = 100).
      //
      // See https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html

      val manifest1 = createStorageManifestWith(
        manifestFiles = Seq(
          createStorageManifestFileWith(name = "alice.txt", size = 100)
        )
      )

      val manifest2 = createStorageManifestWith(
        manifestFiles = Seq(
          createStorageManifestFileWith(name = "alice.txt", size = 200),
          createStorageManifestFileWith(name = "bob.txt", size = 100),
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
            val searchResult1 = searchT[StorageManifest](
              index = manifestsIndex,
              query = nestedQuery(
                "manifest.files",
                must(
                  termQuery("manifest.files.name", "alice.txt")
                )
              )
            )

            searchResult1 should contain theSameElementsAs Seq(manifest1, manifest2)

            val searchResult2 = searchT[StorageManifest](
              index = manifestsIndex,
              query = nestedQuery(
                "manifest.files",
                must(
                  termQuery("manifest.files.name", "alice.txt"),
                  termQuery("manifest.files.size", 100),
                )
              )
            )

            searchResult2 shouldBe Seq(manifest1)
          }
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
