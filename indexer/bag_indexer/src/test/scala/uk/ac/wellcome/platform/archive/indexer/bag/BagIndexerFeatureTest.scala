package uk.ac.wellcome.platform.archive.indexer.bag

import weco.json.JsonUtil._
import weco.storage.BagRegistrationNotification
import weco.storage_service.storage.models.StorageManifest
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
