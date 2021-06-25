package uk.ac.wellcome.platform.archive.indexer.ingests

import weco.json.JsonUtil._
import weco.storage_service.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.ingests.fixtures.IngestsIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

class IngestsIndexerWorkerTest
    extends IndexerWorkerTestCases[Ingest, Ingest, IndexedIngest]
    with IngestsIndexerFixtures
