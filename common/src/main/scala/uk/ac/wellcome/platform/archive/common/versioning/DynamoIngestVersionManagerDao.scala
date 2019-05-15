package uk.ac.wellcome.platform.archive.common.versioning

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.syntax._
import com.gu.scanamo.{Scanamo, Table}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.storage.dynamo._

import scala.util.{Failure, Try}

class DynamoIngestVersionManagerDao(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends IngestVersionManagerDao {

  private val table = Table[VersionRecord](dynamoConfig.table)
  private val index = table.index(dynamoConfig.index)

  override def lookupExistingVersion(ingestId: IngestID): Try[Option[VersionRecord]] = Try {
    val ops = index.query('ingestId -> ingestId)

    Scanamo.exec(dynamoClient)(ops) match {
      case List(Right(record)) => Some(record)
      case Nil                 => None
      case result              => throw new RuntimeException(
        s"Did not find exactly one row with ingest ID $ingestId, got $result"
      )
    }
  }

  override def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]] = Failure(new Throwable("BOOM!"))

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    Scanamo.put(dynamoClient)(table.name)(record) match {
      case Some(Left(err)) => throw new RuntimeException(s"Scanamo error: $err")
      case _ => ()
    }
  }
}
