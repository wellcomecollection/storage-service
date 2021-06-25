package weco.storage_service.indexer.ingests.fixtures

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.indexer.{Indexer, IndexerWorker}
import weco.storage_service.indexer.fixtures.IndexerFixtures
import weco.storage_service.indexer.ingests.{IngestIndexer, IngestsIndexConfig, IngestsIndexerWorker}
import weco.storage_service.indexer.ingests.models.IndexedIngest
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsIndexerFixtures
    extends IndexerFixtures[Ingest, Ingest, IndexedIngest]
    with IngestGenerators { this: Suite =>

  val indexConfig: StorageServiceIndexConfig = IngestsIndexConfig

  def createT: (Ingest, String) = {
    val ingest = createIngest

    (ingest, ingest.id.toString)
  }

  def createIndexer(index: Index): Indexer[Ingest, IndexedIngest] =
    new IngestIndexer(client = elasticClient, index = index)

  override def withIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[IndexerWorker[Ingest, Ingest, IndexedIngest], R]
  )(implicit decoder: Decoder[Ingest]): R = {
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val worker = new IngestsIndexerWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        indexer = createIndexer(index),
        metricsNamespace = "indexer"
      )

      testWith(worker)
    }
  }

  def convertToIndexedT(ingest: Ingest): IndexedIngest = IndexedIngest(ingest)
}
