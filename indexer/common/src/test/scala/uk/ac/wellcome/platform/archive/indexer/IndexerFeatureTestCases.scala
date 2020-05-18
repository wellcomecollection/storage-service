package uk.ac.wellcome.platform.archive.indexer

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.{Decoder, Encoder}
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

abstract class IndexerFeatureTestCases[T, IndexedT](implicit decoderT: Decoder[T], encoderT: Encoder[T], decoderIT: Decoder[IndexedT])
  extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[T, IndexedT] {

  val mapping: MappingDefinition

  def convertIndexedT(t: T): IndexedT

  def createT: (T, String)

  override def createIndexer(index: Index): Indexer[T, IndexedT]

  it("processes a single message") {
    withLocalElasticsearchIndex(mapping) { index =>
      withLocalSqsQueue() { queue =>
        withIndexerWorker(index, queue) { worker =>
          val (t, id) = createT

          worker.run()

          sendNotificationToSQS(queue, t)

          eventually {
            val storedIndexedT = getT[IndexedT](index, id)

            storedIndexedT shouldBe convertIndexedT(t)
          }
        }
      }
    }
  }
}
