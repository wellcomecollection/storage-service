package uk.ac.wellcome.platform.archive.common.ingests.monitor

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.{Scanamo, Table}
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.query.UniqueKey
import com.gu.scanamo.syntax._
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.dynamo.{DynamoConfig, DynamoDao, UpdateExpressionGenerator}
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.{Failure, Success, Try}

class DynamoIngestTracker(
  dynamoDbClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends Logging with IngestTracker {

  implicit val idGetter: IdGetter[Ingest] = (ingest: Ingest) => ingest.id.toString

  implicit val updateExpressionGenerator: UpdateExpressionGenerator[Ingest] = UpdateExpressionGenerator[Ingest]

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

    val ops = underlying.table
      .given(not(attributeExists('id)))
      .update('id -> ingest.id.toString, underlying.buildUpdate(ingest).get)

    underlying.executeOps(id = ingest.id.toString, ops = ops)
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

    if (failures.isEmpty) {
      Failure(new RuntimeException(s"Errors reading from DynamoDB: $failures"))
    } else {
      Success(ingests)
    }
  }
}
