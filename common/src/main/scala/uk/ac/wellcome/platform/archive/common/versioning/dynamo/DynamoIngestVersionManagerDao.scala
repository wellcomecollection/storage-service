package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.syntax._
import com.gu.scanamo.{Scanamo, Table}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, VersionRecord}
import uk.ac.wellcome.storage.dynamo._

import scala.util.{Failure, Success, Try}

class DynamoIngestVersionManagerDao(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends IngestVersionManagerDao {

  private val table = Table[DynamoEntry](dynamoConfig.table)
  private val index = table.index(dynamoConfig.index)

  val hashLookup = new DynamoHashKeyLookup[DynamoEntry, String](
    dynamoClient = dynamoClient,
    lookupConfig = DynamoHashKeyLookupConfig(
      hashKeyName = "id",
      dynamoConfig = dynamoConfig
    )
  )

  override def lookupExistingVersion(
    ingestId: IngestID): Try[Option[VersionRecord]] = {
    val ops = index.query('ingestId -> ingestId)

    Try { Scanamo.exec(dynamoClient)(ops) } match {
      case Success(List(Right(record))) => record.toVersionRecord.map { Some(_) }
      case Success(Nil)                 => Success(None)
      case result => Failure(
        new RuntimeException(
          s"Did not find exactly one row with ingest ID $ingestId, got $result"
        )
      )
    }
  }

  override def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace): Try[Option[VersionRecord]] = {
    val id = DynamoID.createId(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier
    )

    hashLookup.lookupHighestHashKey(id) match {
      case Success(Some(dynamoEntry)) =>
        dynamoEntry.toVersionRecord.map {
          Some(_)
        }
      case Success(None) => Success(None)
      case Failure(err) => Failure(err)
    }
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    Scanamo.put(dynamoClient)(table.name)(DynamoEntry(record)) match {
      case Some(Left(err)) => throw new RuntimeException(s"Scanamo error: $err")
      case _               => ()
    }
  }
}
