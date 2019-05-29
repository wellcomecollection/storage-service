package uk.ac.wellcome.platform.archive.common.ingests.monitor

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, DynamoReadError}
import com.gu.scanamo.syntax._
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.IngestID._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.dynamo._

import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class IngestTracker(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
)(implicit ec: ExecutionContext)
    extends Logging {

  def get(id: IngestID): Future[Option[Ingest]] = Future {
    Scanamo
      .get[Ingest](dynamoClient)(dynamoConfig.table)('id -> id.toString)
      .map {
        case Right(ingest) => ingest
        case Left(err) =>
          throw new RuntimeException(s"Failed to read from DynamoDB: $err")
      }
  }

  def initialise(ingest: Ingest): Try[Ingest] = {
    val ingestTable = Table[Ingest](dynamoConfig.table)
    debug(s"initializing archive ingest tracker with $ingest")

    val ops = ingestTable
      .given(not(attributeExists('id)))
      .put(ingest)

    Try {
      blocking(Scanamo.exec(dynamoClient)(ops)) match {
        case Left(e: ConditionalCheckFailedException) =>
          throw IdConstraintError(
            s"There is already a ingest tracker with id:${ingest.id}",
            e)
        case Left(scanamoError) =>
          val exception = new RuntimeException(
            s"Failed to create ingest ${scanamoError.toString}")
          warn(s"Failed to update Dynamo record: ${ingest.id}", exception)
          throw exception
        case Right(a) =>
          debug(s"Successfully updated Dynamo record: ${ingest.id} $a")
      }
      ingest
    }
  }

  def update(update: IngestUpdate): Try[Ingest] = {
    debug(s"Updating record:${update.id} with:$update")

    val eventsUpdate = appendAll('events -> update.events.toList)

    val mergedUpdate = update match {
      case _: IngestEventUpdate =>
        eventsUpdate
      case statusUpdate: IngestStatusUpdate =>
        val bagUpdate = statusUpdate.affectedBag
          .map(
            bag =>
              set('bag -> bag) and set(
                'bagIdIndex -> bag.toString
            ))
          .toList

        (List(
          eventsUpdate,
          set('status -> statusUpdate.status)
        ) ++ bagUpdate)
          .reduce(_ and _)

      case callbackStatusUpdate: IngestCallbackStatusUpdate =>
        eventsUpdate and set(
          'callback \ 'status -> callbackStatusUpdate.callbackStatus)
    }

    val ingestTable = Table[Ingest](dynamoConfig.table)
    val ops = ingestTable
      .given(attributeExists('id))
      .update('id -> update.id, mergedUpdate)

    Scanamo.exec(dynamoClient)(ops) match {
      case Left(ConditionNotMet(e: ConditionalCheckFailedException)) => {
        val idConstraintError =
          IdConstraintError(
            s"Ingest does not exist for id:${update.id}",
            e
          )

        Failure(idConstraintError)
      }
      case Left(scanamoError) => {
        val exception = new RuntimeException(scanamoError.toString)
        warn(s"Failed to update Dynamo record: ${update.id}", exception)
        Failure(exception)
      }
      case Right(ingest) => {
        debug(s"Successfully updated Dynamo record: ${update.id}, got $ingest")
        Success(ingest)
      }
    }
  }

  /** Find stored ingest given a bagId, uses the secondary index to link a bag to the ingest(s)
    * that created it.
    *
    * This is intended to meet a particular use case for DLCS during migration and not as part of the
    * public/documented API.  Consider either removing this functionality or enhancing it to be fully
    * featured if a use case arises after migration.
    *
    * return a list of Either BagIngest or error querying DynamoDb
    *
    * Returns at most 30 associated ingests with most recent first -- to simplify the code by avoiding
    * pagination, but still fulfilling DLCS's requirements.
    */
  def findByBagId(bagId: BagId): List[Either[DynamoReadError, BagIngest]] = {
    val query = Table[BagIngest](dynamoConfig.table)
      .index(dynamoConfig.index)
      .limit(30)
      .query(('bagIdIndex -> bagId.toString).descending)
    Scanamo.exec(dynamoClient)(query)
  }
}

final case class IdConstraintError(
  private val message: String,
  private val cause: Throwable
) extends Exception(message, cause)
