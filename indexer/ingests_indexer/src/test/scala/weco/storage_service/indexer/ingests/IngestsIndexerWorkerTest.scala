package weco.storage_service.indexer.ingests

import weco.json.JsonUtil._
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.indexer.IndexerWorkerTestCases
import weco.storage_service.indexer.ingests.fixtures.IngestsIndexerFixtures
import weco.storage_service.indexer.ingests.models.IndexedIngest

class IngestsIndexerWorkerTest
    extends IndexerWorkerTestCases[Ingest, Ingest, IndexedIngest]
    with IngestsIndexerFixtures
