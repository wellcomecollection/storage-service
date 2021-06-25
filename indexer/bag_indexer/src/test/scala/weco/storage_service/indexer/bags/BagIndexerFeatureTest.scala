package weco.storage_service.indexer.bags

import weco.json.JsonUtil._
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.storage.models.StorageManifest
import weco.storage_service.indexer.IndexerFeatureTestCases
import weco.storage_service.indexer.bag.fixtures.BagIndexerFixtures
import weco.storage_service.indexer.bags.models.IndexedStorageManifest

class BagIndexerFeatureTest
    extends IndexerFeatureTestCases[
      BagRegistrationNotification,
      StorageManifest,
      IndexedStorageManifest
    ]
    with BagIndexerFixtures
