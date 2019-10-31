package uk.ac.wellcome.platform.storage.bag_versioner.versioning.dynamo

import uk.ac.wellcome.platform.storage.bag_versioner.versioning.IngestVersionManager

class DynamoIngestVersionManager(val dao: DynamoIngestVersionManagerDao)
    extends IngestVersionManager
