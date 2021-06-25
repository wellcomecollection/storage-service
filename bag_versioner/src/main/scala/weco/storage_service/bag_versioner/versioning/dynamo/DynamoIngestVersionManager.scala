package weco.storage_service.bag_versioner.versioning.dynamo

import weco.storage_service.bag_versioner.versioning.IngestVersionManager

class DynamoIngestVersionManager(val dao: DynamoIngestVersionManagerDao)
    extends IngestVersionManager
