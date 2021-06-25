package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.dynamo

import grizzled.slf4j.Logging
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import weco.storage_service.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import weco.storage_service.ingests.models.IngestID._
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.{
  IngestTracker,
  UpdateNonExistentIngestError
}
import uk.ac.wellcome.storage.dynamo._
import weco.storage.store.VersionedStore
import weco.storage.store.dynamo.DynamoSingleVersionStore

import scala.language.higherKinds

class DynamoIngestTracker(config: DynamoConfig)(
  implicit client: DynamoDbClient
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
