package uk.ac.wellcome.platform.archive.indexer.ingests

import java.time.Instant
import java.util.UUID

import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Json
import org.scalatest.Assertion
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestEvent
}
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest
import uk.ac.wellcome.platform.archive.indexer.IndexerTestCases

import scala.concurrent.ExecutionContext.Implicits.global

class IngestIndexerTest
    extends IndexerTestCases[Ingest, ResponseDisplayIngest]
    with IngestGenerators {

  override val mapping: MappingDefinition = IngestsIndexConfig.mapping

  override def createIndexer(
    client: ElasticClient,
    index: Index
  ): IngestIndexer =
    new IngestIndexer(client, index = index)

  override def createDocument: Ingest = createIngest

  override def id(ingest: Ingest): String = ingest.id.toString

  override def assertMatch(
    storedIngest: Map[String, Json],
    ingest: Ingest
  ): Assertion = {
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

  override def createDocumentPair: (Ingest, Ingest) = {
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

    (olderIngest, newerIngest)
  }

  describe("orders updates when one ingest does not have a modified date") {
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
          .index(olderIngest)
          .flatMap { _ =>
            ingestsIndexer.index(newerIngest)
          }

        whenReady(future) { result =>
          result.right.value shouldBe newerIngest

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
          .index(newerIngest)
          .flatMap { _ =>
            ingestsIndexer.index(olderIngest)
          }

        whenReady(future) { result =>
          result.right.value shouldBe olderIngest

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
