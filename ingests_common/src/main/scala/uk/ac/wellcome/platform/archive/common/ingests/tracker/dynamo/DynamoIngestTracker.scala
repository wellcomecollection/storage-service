package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestTracker, IngestTrackerError}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTracker(config: DynamoConfig)(implicit client: AmazonDynamoDB) extends IngestTracker {
  private val hashStore = new DynamoHashStore[IngestID, Int, Ingest](config)

  override val underlying: VersionedStore[IngestID, Int, Ingest] = new VersionedStore[IngestID, Int, Ingest](hashStore)

  override def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]] = ???
}
