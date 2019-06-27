package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.auto._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestTracker, IngestTrackerError}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTracker(val underlying: VersionedStore[IngestID, Int, Ingest]) extends IngestTracker {
  override def listByBagId(bagId:  BagId): Either[IngestTrackerError, Seq[Ingest]] = ???
}

object DynamoIngestTracker {
  def apply(
    config: DynamoConfig)(
    implicit
    dynamoClient: AmazonDynamoDB): DynamoIngestTracker =
    new DynamoIngestTracker(
      new VersionedStore[IngestID, Int, Ingest](
        new DynamoHashStore[Version[IngestID, Int], Int, Ingest](config)
      )
    )
}
