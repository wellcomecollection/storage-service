package uk.ac.wellcome.platform.archive.indexer

import com.sksamuel.elastic4s.ElasticDsl.{properties, textField}
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Successful
}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

abstract class IndexerWorkerTestCases[T, IndexedT](
  implicit
  decoderT: Decoder[T],
  decoderIT: Decoder[IndexedT]
) extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[T, IndexedT]
    with IngestGenerators {

  val mapping: MappingDefinition
  def createT: (T, String)
  def createIndexer(index: Index): Indexer[T, IndexedT]
  def convertToIndexed(t: T): IndexedT

  protected val badMapping: MappingDefinition = properties(
    Seq(textField("name"))
  )

  it("processes a single message") {
    val (t, id) = createT
    withLocalElasticsearchIndex(mapping) { index =>
      val future =
        withIndexerWorker(index) {
          _.process(t)
        }

      whenReady(future) {
        _ shouldBe a[Successful[_]]
      }

      val expectedIndexedT = convertToIndexed(t)
      val actualIndexedT = getT[IndexedT](index, id)

      actualIndexedT shouldBe expectedIndexedT
    }
  }

  it("fails if it cannot index T") {
    val (t, _) = createT

    withLocalElasticsearchIndex(badMapping) { index =>
      val future =
        withIndexerWorker(index) {
          _.process(t)
        }

      whenReady(future) {
        _ shouldBe a[NonDeterministicFailure[_]]
      }
    }
  }
}
