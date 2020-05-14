package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import grizzled.slf4j.Logging
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestTracker
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoSingleVersionStore

class DynamoIngestTracker(config: DynamoConfig)(
  implicit client: AmazonDynamoDB
) extends IngestTracker
    with Logging {

  override val underlying: VersionedStore[IngestID, Int, Ingest] =
    new DynamoSingleVersionStore[IngestID, Ingest](config)
}
