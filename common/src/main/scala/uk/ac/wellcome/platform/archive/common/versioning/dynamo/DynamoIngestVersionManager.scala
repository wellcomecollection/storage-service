package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManager

class DynamoIngestVersionManager(val dao: DynamoIngestVersionManagerDao)
    extends IngestVersionManager
