package uk.ac.wellcome.platform.archive.indexer.bag

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.bag.fixtures.BagIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest

class BagIndexerFeatureTest
    extends IndexerFeatureTestCases[
      BagRegistrationNotification,
      StorageManifest,
      IndexedStorageManifest
    ]
    with BagIndexerFixtures
