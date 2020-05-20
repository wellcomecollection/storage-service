package uk.ac.wellcome.platform.archive.indexer.ingests

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  IndexerWorker
}
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

import scala.concurrent.ExecutionContext.Implicits.global

class IngestsIndexerWorkerTest
    extends IndexerWorkerTestCases[Ingest, Ingest, IndexedIngest]
    with IngestGenerators {

  override val mapping: MappingDefinition = IngestsIndexConfig.mapping

  override def createT: (Ingest, String) = {
    val ingest = createIngest

    (ingest, ingest.id.toString)
  }

  def createIndexer(index: Index): Indexer[Ingest, IndexedIngest] =
    new IngestIndexer(
      client = elasticClient,
      index = index
    )

  override def convertToIndexed(t: Ingest): IndexedIngest =
    IndexedIngest(t)

  override def withIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[IndexerWorker[Ingest, Ingest, IndexedIngest], R]
  )(implicit decoder: Decoder[Ingest]): R = {
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val worker = new IngestsIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          metricsNamespace = "indexer"
        )

        testWith(worker)
      }
    }
  }
}
