package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import grizzled.slf4j.Logging
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.{
  IngestTracker,
  UpdateNonExistentIngestError
}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoSingleVersionStore

class DynamoIngestTracker(config: DynamoConfig)(
  implicit client: AmazonDynamoDB
) extends IngestTracker
    with Logging {

  override val underlying: VersionedStore[IngestID, Int, Ingest] =
    new DynamoSingleVersionStore[IngestID, Ingest](config)

  override def update(update: IngestUpdate): Result =
    super.update(update) match {
      case Left(UpdateNonExistentIngestError(err)) =>
        warn(
          s"DynamoDB could not find ingest ${update.id} to update. " +
            "DynamoDB reads are eventually consistent and this may be fixed on retrying."
        )
        Left(UpdateNonExistentIngestError(err))
      case result => result
    }
}
