package uk.ac.wellcome.platform.archive.indexer.ingests

import java.util.UUID

import com.sksamuel.elastic4s.Index
import io.circe.Json
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

import scala.concurrent.ExecutionContext.Implicits.global

class IngestsIndexerFeatureTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[Ingest, IndexedIngest]
    with IngestGenerators {

  override def createIndexer(index: Index): Indexer[Ingest, IndexedIngest] =
    new IngestIndexer(
      client = elasticClient,
      index = index
    )

  it("processes a single message") {
    val ingest = createIngest

    withLocalElasticsearchIndex(IngestsIndexConfig.mapping) { index =>
      withLocalSqsQueue() { queue =>
        withIndexerWorker(index, queue) { worker =>
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
