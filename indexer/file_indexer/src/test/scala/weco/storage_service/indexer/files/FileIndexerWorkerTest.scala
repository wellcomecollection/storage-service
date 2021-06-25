package weco.storage_service.indexer.files

import weco.json.JsonUtil._
import weco.messaging.worker.models.Successful
import weco.storage_service.indexer.IndexerWorkerTestCases
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.files.fixtures.FileIndexerFixtures
import weco.storage_service.indexer.files.models.IndexedFile

class FileIndexerWorkerTest
    extends IndexerWorkerTestCases[Seq[FileContext], FileContext, IndexedFile]
    with FileIndexerFixtures {

  it("indexes a collection of files") {
    val files = (1 to 10).map { _ =>
      createContext
    }

    withLocalElasticsearchIndex(indexConfig) { index =>
      withIndexerWorker(index) { indexerWorker =>
        whenReady(indexerWorker.process(files)) {
          _ shouldBe a[Successful[_]]
        }

        files.foreach { context =>
          getT[IndexedFile](index, id = context.location.toString()) shouldBe IndexedFile(
            context
          )
        }
      }
    }
  }
}
