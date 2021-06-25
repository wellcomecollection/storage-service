package weco.storage_service.indexer.ingests

import weco.json.JsonUtil._
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.indexer.IndexerFeatureTestCases
import weco.storage_service.indexer.ingests.fixtures.IngestsIndexerFixtures
import weco.storage_service.indexer.ingests.models.IndexedIngest

class IngestsIndexerFeatureTest
    extends IndexerFeatureTestCases[Ingest, Ingest, IndexedIngest]
    with IngestsIndexerFixtures
