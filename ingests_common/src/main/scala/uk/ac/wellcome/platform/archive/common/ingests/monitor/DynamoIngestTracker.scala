package uk.ac.wellcome.platform.archive.common.ingests.monitor

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.gu.scanamo.error.{ConditionNotMet, DynamoReadError, ScanamoError}
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.UniqueKey
import com.gu.scanamo.syntax._
import com.gu.scanamo.update.UpdateExpression
import com.gu.scanamo.{Scanamo, Table}
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.{Failure, Success, Try}

class DynamoIngestTracker(
  dynamoDbClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends Logging with IngestTracker {

  implicit val idGetter: IdGetter[Ingest] = (ingest: Ingest) => ingest.id.toString

  // We need this or we can't construct a DynamoDao, even though
  // it's fiddly to do properly and we don't actually use it.
  implicit val updateExpressionGenerator: UpdateExpressionGenerator[Ingest] = (t: Ingest) => throw new Throwable("This method should never be used")

  val underlying = new DynamoDao[IngestID, Ingest](
    dynamoClient = dynamoDbClient,
    dynamoConfig = dynamoConfig
  ) {
    override protected def buildGetKeyExpression(ident: IngestID): UniqueKey[_] =
      'id -> ident.toString

    override protected def buildPutKeyExpression(ingest: Ingest): UniqueKey[_] =
      'id -> idGetter.id(ingest)
  }

  def get(id: IngestID): Try[Option[Ingest]] = underlying.get(id)

  def initialise(ingest: Ingest): Try[Ingest] = {
    debug(s"initializing archive ingest tracker with $ingest")

    val ops: ScanamoOps[Either[ScanamoError, Ingest]] = underlying.table
      .given(not(attributeExists('id)))
      .put(ingest)
      .map {
        case Right(_) => Right(ingest)
        case Left(err: ConditionalCheckFailedException) => Left(ConditionNotMet(err))
      }

    underlying.executeOps(id = ingest.id.toString, ops = ops)
  }

  def update(update: IngestUpdate): Try[Ingest] = {
    debug(s"Updating record:${update.id} with:$update")

    val eventsUpdate = appendAll('events -> update.events.toList)

    val mergedUpdate: UpdateExpression = update match {
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

    val ops = underlying.table
      .given(attributeExists('id))
      .update('id -> update.id.toString, mergedUpdate)

    underlying.executeOps(id = update.id.toString, ops = ops)
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
  def findByBagId(bagId: BagId): Try[Seq[BagIngest]] = {
    val query = Table[BagIngest](dynamoConfig.table)
      .index(dynamoConfig.index)
      .limit(30)
      .query(('bagIdIndex -> bagId.toString).descending)

    val result: Seq[Either[DynamoReadError, BagIngest]] = Scanamo.exec(dynamoDbClient)(query)

    val ingests = result.collect { case Right(ingest) => ingest }
    val failures = result.collect { case Left(err) => err }

    if (failures.nonEmpty) {
      Failure(new RuntimeException(s"Errors reading from DynamoDB: $failures"))
    } else {
      Success(ingests)
    }
  }
}
