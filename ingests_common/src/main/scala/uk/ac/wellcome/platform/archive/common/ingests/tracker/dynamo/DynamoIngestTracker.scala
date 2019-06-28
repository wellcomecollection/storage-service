package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.DynamoFormat
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestTracker, IngestTrackerError}
import uk.ac.wellcome.storage.dynamo.{DynamoConfig, DynamoHashEntry}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

// TODO: We shouldn't need to get all these implicitly, we should be able
// to construct them here!
class DynamoIngestTracker(config: DynamoConfig)(implicit
  client: AmazonDynamoDB,
  formatIngestID: DynamoFormat[IngestID],
  formatInt: DynamoFormat[Int],
  formatHashEntry: DynamoFormat[DynamoHashEntry[IngestID, Int, Ingest]]
) extends IngestTracker {
  private val hashStore = new DynamoHashStore[IngestID, Int, Ingest](config)

  override val underlying: VersionedStore[IngestID, Int, Ingest] = new VersionedStore[IngestID, Int, Ingest](hashStore)

  override def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]] = ???
}
