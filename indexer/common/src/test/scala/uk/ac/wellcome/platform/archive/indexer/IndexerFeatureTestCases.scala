package uk.ac.wellcome.platform.archive.indexer

import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.{Decoder, Encoder}
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

abstract class IndexerFeatureTestCases[SourceT, T, IndexedT](
  implicit decoderT: Decoder[SourceT],
  encoderT: Encoder[SourceT],
  decoderIT: Decoder[IndexedT]
) extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[SourceT, T, IndexedT] {

  val mapping: MappingDefinition

  def convertToIndexedT(sourceT: SourceT): IndexedT

  def createT: (SourceT, String)

  it("processes a single message") {
    withLocalElasticsearchIndex(mapping) { index =>
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
