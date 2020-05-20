package uk.ac.wellcome.platform.archive.indexer.fixtures

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexerWorker

trait IndexerFixtures[SourceT, T, IndexedT]
    extends ElasticsearchFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with StorageRandomThings { this: Suite =>

  def withIndexerWorker[R](
    index: Index,
    queue: Queue = dummyQueue
  )(testWith: TestWith[IndexerWorker[SourceT, T, IndexedT], R])(
    implicit decoder: Decoder[SourceT]
  ): R
}
