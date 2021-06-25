package weco.storage_service.indexer.fixtures

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.indexer.IndexerWorker

trait IndexerFixtures[SourceT, T, IndexedT]
    extends ElasticsearchFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with StorageRandomGenerators { this: Suite =>

  def withIndexerWorker[R](
    index: Index,
    queue: Queue = dummyQueue
  )(testWith: TestWith[IndexerWorker[SourceT, T, IndexedT], R])(
    implicit decoder: Decoder[SourceT]
  ): R
}
