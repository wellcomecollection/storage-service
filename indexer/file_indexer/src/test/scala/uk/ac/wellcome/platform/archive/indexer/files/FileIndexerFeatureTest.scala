package uk.ac.wellcome.platform.archive.indexer.files

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.files.fixtures.FileIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.files.models.IndexedFile

class FileIndexerFeatureTest
  extends IndexerFeatureTestCases[FileContext, FileContext, IndexedFile]
    with FileIndexerFixtures
