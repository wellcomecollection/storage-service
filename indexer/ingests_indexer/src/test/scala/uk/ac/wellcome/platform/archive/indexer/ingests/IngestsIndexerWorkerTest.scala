package uk.ac.wellcome.platform.archive.indexer.ingests

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

import scala.concurrent.ExecutionContext.Implicits.global

class IngestsIndexerWorkerTest
    extends IndexerWorkerTestCases[Ingest, IndexedIngest] {

  override val mapping: MappingDefinition = IngestsIndexConfig.mapping

  override def createT: (Ingest, String) = {
    val ingest = createIngest

    (ingest, ingest.id.toString)
  }

  override def createIndexer(index: Index): Indexer[Ingest, IndexedIngest] =
    new IngestIndexer(
      client = elasticClient,
      index = index
    )

}
