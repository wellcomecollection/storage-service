package uk.ac.wellcome.platform.archive.indexer.bag

import java.time.Instant

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.ElasticClientFactory
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class ManifestIndexerTest extends FunSpec with Matchers with EitherValues with ElasticsearchFixtures with StorageManifestGenerators {
  it("indexes a single manifest") {
    withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
      val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

      val manifest = createStorageManifest

      whenReady(manifestIndexer.index(manifest)) { result =>
        result.right.value shouldBe manifest

        val storedManifest =
          getT[Json](index, id = manifest.idWithVersion)
            .as[Map[String, Json]]
            .right
            .value

        val storedId = storedManifest("id").asString.get
        storedId shouldBe manifest.id.toString
      }
    }
  }

  it("indexes multiple manifests") {
    withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
      val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

      val manifestCount = 10
      val manifests = (1 to manifestCount).map { _ =>
        createStorageManifest
      }

      whenReady(manifestIndexer.index(manifests)) { result =>
        result.right.value shouldBe manifests

        eventually {
          val storedManifests = searchT[Json](index, query = matchAllQuery())

          storedManifests should have size manifestCount

          val storedIds =
            storedManifests
              .map { _.as[Map[String, Json]].right.value }
              .map { _("id").asString.get }

          storedIds shouldBe manifests.map { _.id.toString }
        }
      }
    }
  }

  it("returns a Left if the document can't be indexed correctly") {
    val manifest = createStorageManifest

    val badMapping = properties(
      Seq(textField("name"))
    )

    withLocalElasticsearchIndex(badMapping) { index =>
      val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

      whenReady(manifestIndexer.index(manifest)) {
        _.left.value shouldBe manifest
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
    val manifestIndexer = new ManifestIndexer(badClient, index = index)

    val manifest = createStorageManifest

    whenReady(manifestIndexer.index(manifest).failed) {
      _ shouldBe a[JavaClientExceptionWrapper]
    }
  }

  describe("orders updates correctly") {
    describe("when both manifests have a modified date") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val olderManifest = createStorageManifestWith(
        space = space,
        bagInfo = createBagInfoWith(
          externalIdentifier = externalIdentifier
        ),
        createdDate = Instant.ofEpochMilli(1)
      )

      val newerManifest = createStorageManifestWith(
        space = space,
        bagInfo = createBagInfoWith(
          externalIdentifier = externalIdentifier
        ),
        createdDate = Instant.ofEpochMilli(2)
      )

      it("a newer ingest replaces an older ingest") {
        withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
          val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

          val future = manifestIndexer
            .index(olderManifest)
            .flatMap { _ =>
              manifestIndexer.index(newerManifest)
            }

          whenReady(future) { result =>
            result.right.value shouldBe newerManifest

            val storedManifest =
              getT[Json](index, id = olderManifest.idWithVersion)
                .as[Map[String, Json]]
                .right
                .value

            val storedId = storedManifest("id").asString.get
            storedId shouldBe olderManifest.id.toString
          }
        }
      }

      it("an older ingest does not replace a newer ingest") {
        withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
          val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

          val future = manifestIndexer
            .index(newerManifest)
            .flatMap { _ =>
              manifestIndexer.index(olderManifest)
            }

          whenReady(future) { result =>
            result.right.value shouldBe olderManifest

            val storedManifest =
              getT[Json](index, id = olderManifest.idWithVersion)
                .as[Map[String, Json]]
                .right
                .value

            val storedId = storedManifest("id").asString.get
            storedId shouldBe olderManifest.id.toString
          }
        }
      }
    }

    describe("when one ingest does not have a modified date") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val olderManifest = createStorageManifestWith(
        space = space,
        bagInfo = createBagInfoWith(
          externalIdentifier = externalIdentifier
        ),
        createdDate = Instant.ofEpochMilli(1)
      )

      val newerManifest = createStorageManifestWith(
        space = space,
        bagInfo = createBagInfoWith(
          externalIdentifier = externalIdentifier
        ),
        createdDate = Instant.ofEpochMilli(2)
      )

      it("a newer ingest replaces an older ingest") {
        withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
          val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

          val future = manifestIndexer
            .index(olderManifest)
            .flatMap { _ =>
              manifestIndexer.index(newerManifest)
            }

          whenReady(future) { result =>
            result.right.value shouldBe newerManifest

            val storedManifest =
              getT[Json](index, id = olderManifest.idWithVersion)
                .as[Map[String, Json]]
                .right
                .value

            val storedId = storedManifest("id").asString.get
            storedId shouldBe olderManifest.id.toString
          }
        }
      }

      it("an older ingest does not replace a newer ingest") {
        withLocalElasticsearchIndex(ManifestsIndexConfig.mapping) { index =>
          val manifestIndexer = new ManifestIndexer(elasticClient, index = index)

          val future = manifestIndexer
            .index(newerManifest)
            .flatMap { _ =>
              manifestIndexer.index(olderManifest)
            }

          whenReady(future) { result =>
            result.right.value shouldBe olderManifest

            val storedManifest =
              getT[Json](index, id = olderManifest.idWithVersion)
                .as[Map[String, Json]]
                .right
                .value

            val storedId = storedManifest("id").asString.get
            storedId shouldBe olderManifest.id.toString
          }
        }
      }
    }
  }
}
