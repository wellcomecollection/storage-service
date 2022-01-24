package weco.storage_service.indexer

import com.sksamuel.elastic4s.ElasticDsl.{properties, textField}
import com.sksamuel.elastic4s.analysis.Analysis
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import io.circe.Decoder
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.elasticsearch.IndexConfig
import weco.messaging.worker.models.{RetryableFailure, Successful}
import weco.storage_service.indexer.fixtures.IndexerFixtures
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

abstract class IndexerWorkerTestCases[SourceT, T, IndexedT](
  implicit
  decoderT: Decoder[SourceT],
  decoderIT: Decoder[IndexedT]
) extends AnyFunSpec
    with Matchers
    with EitherValues
    with IndexerFixtures[SourceT, T, IndexedT] {

  // TODO: Cover the code path where "load" is called
  // We should have tests to test failure modes in load
  // If this code is shared with the catalogue we should add those.

  val indexConfig: StorageServiceIndexConfig

  def createT: (SourceT, String)

  def convertToIndexedT(sourceT: SourceT): IndexedT

  val badConfig: IndexConfig = IndexConfig(
    mapping = properties(Seq(textField("name")))
      .dynamic(DynamicMapping.Strict),
    analysis = Analysis(analyzers = List())
  )

  it("processes a single message") {
    val (t, id) = createT
    withLocalElasticsearchIndex(indexConfig) { index =>
      withIndexerWorker(index) { indexerWorker =>
        whenReady(indexerWorker.process(t)) { result =>
          result shouldBe a[Successful[_]]
          val expectedIndexedT = convertToIndexedT(t)
          val actualIndexedT = getT[IndexedT](index, id)

          actualIndexedT shouldBe expectedIndexedT
        }
      }

      val expectedIndexedT = convertToIndexedT(t)
      val actualIndexedT = getT[IndexedT](index, id)

      actualIndexedT shouldBe expectedIndexedT
    }
  }

  it("fails if it cannot index T") {
    val (t, _) = createT

    withLocalElasticsearchIndex(badConfig) { index =>
      withIndexerWorker(index) { worker =>
        whenReady(worker.process(t)) {
          _ shouldBe a[RetryableFailure[_]]
        }
      }
    }
  }
}
