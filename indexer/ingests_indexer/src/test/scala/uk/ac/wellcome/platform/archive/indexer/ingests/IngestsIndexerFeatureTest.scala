package uk.ac.wellcome.platform.archive.indexer.ingests
import java.util.UUID

import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.indexer.ingests.fixtures.IngestsIndexerFixtures

class IngestsIndexerFeatureTest
    extends FunSpec
    with Matchers
    with EitherValues
    with IngestsIndexerFixtures
    with IngestGenerators {
  it("processes a single message") {
    val ingest = createIngest

    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      withLocalSqsQueue { queue =>
        withIngestsIndexerWorker(queue, index) { worker =>
          worker.run()

          sendNotificationToSQS(queue, ingest)

          eventually {
            val storedIngest =
              getT[Json](index, id = ingest.id.toString)
                .as[Map[String, Json]]
                .right
                .value

            val storedIngestId =
              UUID.fromString(storedIngest("id").asString.get)
            storedIngestId shouldBe ingest.id.underlying
          }
        }
      }
    }
  }
}
