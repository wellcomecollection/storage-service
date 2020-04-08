package uk.ac.wellcome.platform.archive.indexer.ingests.fixtures

import com.sksamuel.elastic4s.Index
import org.scalatest.Suite
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.MonitoringClientFixture
import uk.ac.wellcome.platform.archive.indexer.fixtures.ElasticsearchFixtures
import uk.ac.wellcome.platform.archive.indexer.ingests.{IngestIndexer, IngestsIndexerWorker}

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsIndexerFixtures extends ElasticsearchFixtures with Akka with AlpakkaSQSWorkerFixtures with MonitoringClientFixture { this: Suite =>
  def withIngestsIndexerWorker[R](
    queue: Queue = Queue("queue://test", "arn::test"),
    index: Index
  )(testWith: TestWith[IngestsIndexerWorker, R]): R = {
    val ingestIndexer = new IngestIndexer(
      client = elasticClient,
      index = index
    )

    withActorSystem { implicit actorSystem =>
      withMonitoringClient { implicit monitoringClient =>
        val worker = new IngestsIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          ingestIndexer = ingestIndexer
        )

        testWith(worker)
      }
    }
  }
}
