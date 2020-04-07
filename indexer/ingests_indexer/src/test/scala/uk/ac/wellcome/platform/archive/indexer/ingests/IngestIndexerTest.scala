package uk.ac.wellcome.platform.archive.indexer.ingests

import java.util.UUID

import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.ElasticClientFactory
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class IngestIndexerTest
  extends FunSpec
    with Matchers
    with EitherValues
    with IngestGenerators
    with ElasticsearchFixtures {

  it("indexes an ingest") {
    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      val ingestsIndexer = new IngestIndexer(elasticClient, index = index)

      val ingest = createIngest

      whenReady(ingestsIndexer.index(Seq(ingest))) { result =>
        result.right.value shouldBe Seq(ingest)

        val storedIngest =
          getT[Json](index, id = ingest.id.toString)
            .as[Map[String, Json]]
            .right.value

        val storedIngestId = UUID.fromString(storedIngest("id").asString.get)
        storedIngestId shouldBe ingest.id.underlying

        val storedExternalIdentifier = ExternalIdentifier(
          storedIngest("bag")
            .asObject.get
            .toMap("info").asObject.get
            .toMap("externalIdentifier").asString.get
        )
        storedExternalIdentifier shouldBe ingest.externalIdentifier
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
}
