package weco.storage_service.indexer.files

import weco.json.JsonUtil._
import weco.storage_service.indexer.IndexerFeatureTestCases
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.files.fixtures.FileIndexerFixtures
import weco.storage_service.indexer.files.models.IndexedFile

class FileIndexerFeatureTest
    extends IndexerFeatureTestCases[Seq[FileContext], FileContext, IndexedFile]
    with FileIndexerFixtures
