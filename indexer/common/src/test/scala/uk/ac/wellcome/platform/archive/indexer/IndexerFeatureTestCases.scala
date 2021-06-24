package uk.ac.wellcome.platform.archive.indexer

import io.circe.{Decoder, Encoder}
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.StorageServiceIndexConfig
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

abstract class IndexerFeatureTestCases[SourceT, T, IndexedT](
  implicit decoderT: Decoder[SourceT],
  encoderT: Encoder[SourceT],
  decoderIT: Decoder[IndexedT]
) extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[SourceT, T, IndexedT] {

  val indexConfig: StorageServiceIndexConfig

  def convertToIndexedT(sourceT: SourceT): IndexedT

  def createT: (SourceT, String)

  it("processes a single message") {
    withLocalElasticsearchIndex(indexConfig) { index =>
      withLocalSqsQueue() { queue =>
        withIndexerWorker(index, queue) { worker =>
          val (t, id) = createT

          worker.run()

          sendNotificationToSQS(queue, t)

          eventually {
            val storedIndexedT = getT[IndexedT](index, id)

            storedIndexedT shouldBe convertToIndexedT(t)
          }
        }
      }
    }
  }
}
