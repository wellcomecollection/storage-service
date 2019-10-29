package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import grizzled.slf4j.Logging
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTracker(config: DynamoConfig)(
  implicit client: AmazonDynamoDB
) extends IngestTracker
    with Logging {

  // TODO: This should be upstreamed to the scala-storage library
  private val hashStore = new DynamoHashStore[IngestID, Int, Ingest](config) {
    override def max(hashKey: IngestID): Either[ReadError, Int] =
      super.max(hashKey) match {
        case Right(value)               => Right(value)
        case Left(_: DoesNotExistError) => Left(NoMaximaValueError())
        case Left(err)                  => Left(err)
      }
  }

  override val underlying: VersionedStore[IngestID, Int, Ingest] =
    new VersionedStore[IngestID, Int, Ingest](hashStore)
}
