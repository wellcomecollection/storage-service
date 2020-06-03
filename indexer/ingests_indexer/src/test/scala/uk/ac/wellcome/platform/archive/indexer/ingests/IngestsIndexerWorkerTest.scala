package uk.ac.wellcome.platform.archive.indexer.ingests

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.ingests.fixtures.IngestsIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

class IngestsIndexerWorkerTest
    extends IndexerWorkerTestCases[Ingest, Ingest, IndexedIngest]
    with IngestsIndexerFixtures
    with IngestGenerators
