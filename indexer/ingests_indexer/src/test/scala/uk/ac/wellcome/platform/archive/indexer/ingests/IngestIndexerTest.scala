package uk.ac.wellcome.platform.archive.indexer.ingests

import java.time.Instant
import java.util.UUID

import com.sksamuel.elastic4s.ElasticDsl.{matchAllQuery, properties, textField}
import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestEvent
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.ElasticClientFactory
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class IngestIndexerTest
    extends FunSpec
    with Matchers
    with EitherValues
    with IngestGenerators
    with ElasticsearchFixtures {

  it("indexes a single ingest") {
    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

      val ingest = createIngest

      whenReady(ingestsIndexer.index(Seq(ingest))) { result =>
        result.right.value shouldBe Seq(ingest)

        val storedIngest =
          getT[Json](index, id = ingest.id.toString)
            .as[Map[String, Json]]
            .right
            .value

        val storedIngestId = UUID.fromString(storedIngest("id").asString.get)
        storedIngestId shouldBe ingest.id.underlying

        val storedExternalIdentifier = ExternalIdentifier(
          storedIngest("bag").asObject.get
            .toMap("info")
            .asObject
            .get
            .toMap("externalIdentifier")
            .asString
            .get
        )
        storedExternalIdentifier shouldBe ingest.externalIdentifier
      }
    }
  }

  it("indexes multiple ingests") {
    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

      val ingestCount = 10
      val ingests = (1 to ingestCount).map { _ =>
        createIngest
      }

      whenReady(ingestsIndexer.index(ingests)) { result =>
        result.right.value shouldBe ingests

        eventually {
          val storedIngests = searchT[Json](index, query = matchAllQuery())

          storedIngests should have size ingestCount

          val storedIds =
            storedIngests
              .map { _.as[Map[String, Json]].right.value }
              .map { _("id").asString.get }

          storedIds shouldBe ingests.map { _.id.toString }
        }
      }
    }
  }

  it("returns a Left if the document can't be indexed correctly") {
    val ingest = createIngest

    val badMapping = properties(
      Seq(textField("name"))
    )

    withLocalElasticsearchIndex(badMapping) { index =>
      val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

      whenReady(ingestsIndexer.index(Seq(ingest))) {
        _.left.value shouldBe Seq(ingest)
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
    val ingestsIndexer = new IngestIndexer(badClient, index = index)

    val ingest = createIngest

    whenReady(ingestsIndexer.index(Seq(ingest)).failed) {
      _ shouldBe a[JavaClientExceptionWrapper]
    }
  }

  describe("orders updates correctly") {
    describe("when both ingests have a modified date") {
      val ingestId = createIngestID

      val olderIngest = createIngestWith(
        id = ingestId,
        events = Seq(
          IngestEvent(
            description = "event 1",
            createdDate = Instant.ofEpochMilli(101)
          )
        ),
        createdDate = Instant.ofEpochMilli(1)
      )

      val newerIngest = olderIngest.copy(
        events = Seq(
          IngestEvent(
            description = "event 1",
            createdDate = Instant.ofEpochMilli(101)
          ),
          IngestEvent(
            description = "event 2",
            createdDate = Instant.ofEpochMilli(102)
          )
        ),
        createdDate = Instant.ofEpochMilli(2)
      )

      assert(
        olderIngest.lastModifiedDate.get
          .isBefore(newerIngest.lastModifiedDate.get)
      )

      it("a newer ingest replaces an older ingest") {
        withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
          val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

          val future = ingestsIndexer
            .index(Seq(olderIngest))
            .flatMap { _ =>
              ingestsIndexer.index(Seq(newerIngest))
            }

          whenReady(future) { result =>
            result.right.value shouldBe Seq(newerIngest)

            val storedIngest =
              getT[Json](index, id = ingestId.toString)
                .as[Map[String, Json]]
                .right
                .value

            storedIngest("events").asArray.get.size shouldBe 2
          }
        }
      }

      it("an older ingest does not replace a newer ingest") {
        withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
          val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

          val future = ingestsIndexer
            .index(Seq(newerIngest))
            .flatMap { _ =>
              ingestsIndexer.index(Seq(olderIngest))
            }

          whenReady(future) { result =>
            result.right.value shouldBe Seq(olderIngest)

            val storedIngest =
              getT[Json](index, id = ingestId.toString)
                .as[Map[String, Json]]
                .right
                .value

            storedIngest("events").asArray.get.size shouldBe 2
          }
        }
      }
    }

    describe("when one ingest does not have a modified date") {
      val ingestId = createIngestID

      val olderIngest = createIngestWith(
        id = ingestId,
        events = Seq.empty,
        createdDate = Instant.ofEpochMilli(1)
      )

      val newerIngest = olderIngest.copy(
        events = Seq(
          IngestEvent(
            description = "event 1",
            createdDate = Instant.ofEpochMilli(101)
          ),
          IngestEvent(
            description = "event 2",
            createdDate = Instant.ofEpochMilli(102)
          )
        ),
        createdDate = Instant.ofEpochMilli(2)
      )

      assert(olderIngest.lastModifiedDate.isEmpty)
      assert(newerIngest.lastModifiedDate.isDefined)

      it("a newer ingest replaces an older ingest") {
        withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
          val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

          val future = ingestsIndexer
            .index(Seq(olderIngest))
            .flatMap { _ =>
              ingestsIndexer.index(Seq(newerIngest))
            }

          whenReady(future) { result =>
            result.right.value shouldBe Seq(newerIngest)

            val storedIngest =
              getT[Json](index, id = ingestId.toString)
                .as[Map[String, Json]]
                .right
                .value

            storedIngest("events").asArray.get.size shouldBe 2
          }
        }
      }

      it("an older ingest does not replace a newer ingest") {
        withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
          val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

          val future = ingestsIndexer
            .index(Seq(newerIngest))
            .flatMap { _ =>
              ingestsIndexer.index(Seq(olderIngest))
            }

          whenReady(future) { result =>
            result.right.value shouldBe Seq(olderIngest)

            val storedIngest =
              getT[Json](index, id = ingestId.toString)
                .as[Map[String, Json]]
                .right
                .value

            storedIngest("events").asArray.get.size shouldBe 2
          }
        }
      }
    }
  }
}
