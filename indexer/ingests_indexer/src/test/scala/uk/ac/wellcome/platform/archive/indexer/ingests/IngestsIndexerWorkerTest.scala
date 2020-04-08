package uk.ac.wellcome.platform.archive.indexer.ingests

import java.util.UUID

import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.messaging.worker.models.Successful
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.indexer.ingests.fixtures.IngestsIndexerFixtures

class IngestsIndexerWorkerTest extends FunSpec with Matchers with EitherValues with IngestsIndexerFixtures with IngestGenerators {
  it("processes a single message") {
    val ingest = createIngest

    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      val future =
        withIngestsIndexerWorker(index = index) {
          _.process(ingest)
        }

      whenReady(future) {
        _ shouldBe a[Successful[_]]
      }

      val storedIngest =
        getT[Json](index, id = ingest.id.toString)
          .as[Map[String, Json]]
          .right
          .value

      val storedIngestId = UUID.fromString(storedIngest("id").asString.get)
      storedIngestId shouldBe ingest.id.underlying
    }
  }
}
