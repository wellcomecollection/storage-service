package uk.ac.wellcome.platform.archive.indexer.fixtures

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  IndexerWorker
}

import scala.concurrent.ExecutionContext.Implicits.global

trait IndexerFixtures[T, IndexedT]
    extends ElasticsearchFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with StorageRandomThings { this: Suite =>
  def withIndexerWorker[R](
    index: Index,
    indexer: Index => Indexer[T, IndexedT],
    queue: Queue = dummyQueue
  )(testWith: TestWith[IndexerWorker[T, IndexedT], R])(
    implicit decoder: Decoder[T]
  ): R = {
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val worker = new IndexerWorker[T, IndexedT](
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = indexer(index),
          metricsNamespace = "indexer"
        )

        testWith(worker)
      }
    }
  }
}
