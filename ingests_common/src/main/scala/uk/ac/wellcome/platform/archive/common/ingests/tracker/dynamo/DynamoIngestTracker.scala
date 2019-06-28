package uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.{Scanamo, Table => ScanamoTable}
import org.scanamo.auto._
import org.scanamo.syntax._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestTracker, IngestTrackerError, IngestTrackerStoreError}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

class DynamoIngestTracker(config: DynamoConfig)(implicit client: AmazonDynamoDB) extends IngestTracker {

  // TODO: This should be upstreamed to the scala-storage library
  private val hashStore = new DynamoHashStore[IngestID, Int, Ingest](config) {
    override def max(hashKey: IngestID): Either[ReadError, Int] =
      super.max(hashKey) match {
        case Right(value) => Right(value)
        case Left(_: DoesNotExistError) => Left(NoMaximaValueError())
        case Left(err) => Left(err)
      }

    override def put(id: Version[IngestID, Int])(ingest: Ingest): WriteEither =
      super.put(id)(ingest).flatMap { result: Identified[Version[IngestID, Int], Ingest] =>
        ingest.bag match {
          case Some(bagId) =>
            val ops = table
              .given(attributeExists('id) and 'version = result.id.version)
              .update('bagIdIndex -> bagId)
        }

      }
  }

  override val underlying: VersionedStore[IngestID, Int, Ingest] = new VersionedStore[IngestID, Int, Ingest](hashStore)

  override def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]] = {


    val query = ScanamoTable[Ingest](config.tableName)
      .index(config.indexName)
//      .limit(30)
      .query('bagId \ 'externalIdentifier -> bagId.externalIdentifier)
//      .descending

    val result = Scanamo(client).exec(query)

    val ingests = result.collect { case Right(ingest) => ingest }
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
