package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Index
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_indexer.fixtures.ElasticsearchFixtures
import uk.ac.wellcome.platform.archive.bag_indexer.models.IndexedFile
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagIndexerTest extends FunSpec with Matchers with ScalaFutures with Eventually with IntegrationPatience with ElasticsearchFixtures with StorageManifestGenerators {
  describe("indexing storage manifests") {
    it("indexes a single manifest") {
      val manifest = createStorageManifestWith(
        manifestFiles = Seq(
          createStorageManifestFileWith(path = "v1/alice.txt"),
          createStorageManifestFileWith(path = "v1/bob.txt"),
          createStorageManifestFileWith(path = "v1/carol.txt"),
        ),
        version = BagVersion(1),
      )

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
          _ shouldBe a[Throwable]
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
          createStorageManifestFileWith(path = "v1/alice.txt", size = 100)
        ),
        version = BagVersion(1)
      )

      val manifest2 = createStorageManifestWith(
        manifestFiles = Seq(
          createStorageManifestFileWith(path = "v1/alice.txt", size = 200),
          createStorageManifestFileWith(path = "v1/bob.txt", size = 100),
        ),
        version = BagVersion(1)
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
                  termQuery("manifest.files.path", "v1/alice.txt")
                )
              )
            )

            searchResult1 should contain theSameElementsAs Seq(manifest1, manifest2)

            val searchResult2 = searchT[StorageManifest](
              index = manifestsIndex,
              query = nestedQuery(
                "manifest.files",
                must(
                  termQuery("manifest.files.path", "v1/alice.txt"),
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

  describe("indexing individual files") {
    val fileA = createStorageManifestFileWith(path = "v1/alice.txt")
    val fileB = createStorageManifestFileWith(path = "v1/bob.txt")
    val fileC = createStorageManifestFileWith(path = "v1/carol.txt")

    val manifest = createStorageManifestWith(
      manifestFiles = Seq(fileA, fileB, fileC),
      version = BagVersion(1),
    )

    val manifestAtV2 = createStorageManifestWith(
      manifestFiles = Seq(fileA, fileB, fileC),
      location = manifest.location,
      version = BagVersion(2),
    )

    it("indexes the individual files") {
      withIndexes { case (manifestsIndex, filesIndex) =>
        val future =
          withBagIndexer(manifestsIndex, filesIndex) {
            _.index(manifest)
          }

        whenReady(future) { _ =>
          val storedFile = getT[IndexedFile](
            filesIndex, id = s"s3://${manifest.location.prefix}/${fileA.path}"
          )

          storedFile shouldBe IndexedFile(manifest, fileA)
        }
      }
    }

    describe("tracks the versions of a bag that a file appears in") {
      val indexedFile = IndexedFile(manifest, fileA)
      val expectedIndexedFile =
        indexedFile
          .copy(bag = indexedFile.bag.copy(versions = Seq("v1", "v2")))

      it("a single version") {
        withIndexes { case (manifestsIndex, filesIndex) =>
          val future =
            withBagIndexer(manifestsIndex, filesIndex) { bagIndexer =>
              bagIndexer.index(manifest)
                .flatMap { _ =>
                  bagIndexer.index(manifestAtV2)
                }
            }

          whenReady(future) { _ =>
            val storedFile = getT[IndexedFile](
              filesIndex, id = s"s3://${manifest.location.prefix}/${fileA.path}"
            )

            storedFile shouldBe expectedIndexedFile
          }
        }
      }

      it("de-duplicates versions")  {
        withIndexes { case (manifestsIndex, filesIndex) =>
          // Suppose that, for some reason, we indexed the same bag three times.
          // We should only record "v2" once in the versions list.
          val future =
            withBagIndexer(manifestsIndex, filesIndex) { bagIndexer =>
              bagIndexer.index(manifest)
                .flatMap { _ => bagIndexer.index(manifestAtV2) }
                .flatMap { _ => bagIndexer.index(manifestAtV2) }
                .flatMap { _ => bagIndexer.index(manifestAtV2) }
            }


          whenReady(future) { _ =>
            val storedFile = getT[IndexedFile](
              filesIndex, id = s"s3://${manifest.location.prefix}/${fileA.path}"
            )

            storedFile shouldBe expectedIndexedFile
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
