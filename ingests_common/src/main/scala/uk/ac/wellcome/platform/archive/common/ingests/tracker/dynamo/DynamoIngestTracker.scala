package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import java.time.Instant

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import grizzled.slf4j.Logging
import org.scanamo.{Scanamo, Table => ScanamoTable}
import org.scanamo.auto._
import org.scanamo.error.DynamoReadError
import org.scanamo.syntax._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestTracker,
  IngestTrackerError,
  IngestTrackerStoreError
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

import scala.util.Try

class DynamoIngestTracker(
  config: DynamoConfig,
  bagIdLookupConfig: DynamoConfig)(implicit client: AmazonDynamoDB)
    extends IngestTracker
    with Logging {

  // TODO: This should be upstreamed to the scala-storage library
  private val hashStore = new DynamoHashStore[IngestID, Int, Ingest](config) {
    override def max(hashKey: IngestID): Either[ReadError, Int] =
      super.max(hashKey) match {
        case Right(value) => Right(value)
        case Left(_: DoesNotExistError) => Left(NoMaximaValueError())
        case Left(err) => Left(err)
      }

    override def put(id: Version[IngestID, Int])(ingest: Ingest): WriteEither =
      super.put(id)(ingest).map { result =>
        debug(s"Storing bagID lookup: ${storeBagIdLookup(ingest)}")
        result
      }
  }

  override val underlying: VersionedStore[IngestID, Int, Ingest] =
    new VersionedStore[IngestID, Int, Ingest](hashStore)

  // The bag ID lookup is a temporary feature for DLCS during the migration.
  // For now we're splitting the data across two tables because you can't
  // create GSIs on nested attributes, and the vanilla DynamoStore puts
  // everything except the ingest ID in a nested attribute.
  //
  // If we keep this feature we should go back and add support for nested,
  // attributes, but for now this is a quick fix.

  case class BagIdLookup(
    bagId: String,
    ingestDate: Instant,
    ingest: Ingest
  )

  private def storeBagIdLookup(
    ingest: Ingest): Try[Option[Either[DynamoReadError, BagIdLookup]]] = {
    val ops = ScanamoTable[BagIdLookup](bagIdLookupConfig.tableName)
      .put(
        BagIdLookup(
          bagId = BagId(ingest.space, ingest.externalIdentifier).toString,
          ingestDate = ingest.createdDate,
          ingest = ingest
        )
      )

    Try { Scanamo(client).exec(ops) }
  }

  override def listByBagId(
    bagId: BagId): Either[IngestTrackerError, Seq[Ingest]] = {
    val query = ScanamoTable[BagIdLookup](bagIdLookupConfig.tableName)
      .limit(30)
      .descending
      .query('bagId -> bagId.toString)

    val result = Scanamo(client).exec(query)

    val ingests = result.collect {
      case Right(bagIdLookup) => bagIdLookup.ingest
    }
    val errors = result.collect { case Left(err) => err }

    Either.cond(
      errors.isEmpty,
      right = ingests,
      left = IngestTrackerStoreError(
        StoreReadError(new Throwable(s"Errors from DynamoDB: $errors"))
      )
    )
  }
}
