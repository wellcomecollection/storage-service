package uk.ac.wellcome.platform.archive.indexer.files

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.worker.models.Successful
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.files.fixtures.FileIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.files.models.IndexedFile

class FileIndexerWorkerTest
    extends IndexerWorkerTestCases[Seq[FileContext], FileContext, IndexedFile]
    with FileIndexerFixtures {

  it("indexes a collection of files") {
    val files = (1 to 10).map { _ => createContext}

    withLocalElasticsearchIndex(mapping) { index =>
      withIndexerWorker(index) { indexerWorker =>
        whenReady(indexerWorker.process(files)) {
          _ shouldBe a[Successful[_]]
        }

        files.foreach { context =>
          getT[IndexedFile](index, id = context.location.toString()) shouldBe IndexedFile(context)
        }
      }
    }
  }
}
