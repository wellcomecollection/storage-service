package uk.ac.wellcome.platform.archive.indexer.ingests

import weco.json.JsonUtil._
import weco.storage_service.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.ingests.fixtures.IngestsIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

class IngestsIndexerFeatureTest
    extends IndexerFeatureTestCases[Ingest, Ingest, IndexedIngest]
    with IngestsIndexerFixtures
