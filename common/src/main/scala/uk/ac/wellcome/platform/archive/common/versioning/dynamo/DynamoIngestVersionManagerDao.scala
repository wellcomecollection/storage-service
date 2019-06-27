package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.auto._
import org.scanamo.syntax._
import org.scanamo.time.JavaTimeFormats._
import org.scanamo.{DynamoFormat, Scanamo, Table => ScanamoTable}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, VersionRecord}
import uk.ac.wellcome.storage.MaximaError
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.maxima.dynamo.DynamoHashRangeMaxima

import scala.util.{Failure, Success, Try}

class DynamoIngestVersionManagerDao(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends IngestVersionManagerDao {

  private val scanamoTable = ScanamoTable[DynamoVersionRecord](dynamoConfig.tableName)
  private val index = scanamoTable.index(dynamoConfig.indexName)

  val maxima = new DynamoHashRangeMaxima[String, Int, DynamoVersionRecord] {
    override protected implicit val formatHashKey: DynamoFormat[String] = DynamoFormat[String]
    override protected implicit val formatRangeKey: DynamoFormat[Int] = DynamoFormat[Int]
    override protected implicit val format: DynamoFormat[DynamoVersionRecord] = DynamoFormat[DynamoVersionRecord]
    override protected val client: AmazonDynamoDB = dynamoClient
    override protected val table: ScanamoTable[DynamoVersionRecord] = scanamoTable
  }

  override def lookupExistingVersion(
    ingestId: IngestID): Try[Option[VersionRecord]] = {
    val ops = index.query('ingestId -> ingestId)

    Try { Scanamo(dynamoClient).exec(ops) } match {
      case Success(List(Right(record))) =>
        record.toVersionRecord.map { Some(_) }
      case Success(Nil) => Success(None)
      case Success(result) =>
        Failure(
          new RuntimeException(
            s"Did not find exactly one row with ingest ID $ingestId, got $result"
          )
        )

      case Failure(err) => Failure(err)
    }
  }

  override def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace): Either[MaximaError, VersionRecord] = {
    val id = DynamoID.createId(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier
    )

    // TODO Because the maxima only returns the version and not the full entry,
    // we go back and make a second GET.  This is daft and we need to sort out
    // the maxima, hence doing this in an icky way.
    maxima.max(id).map { maxVersion =>
      val ops = scanamoTable.get('id -> id and 'version -> maxVersion)
      Scanamo(dynamoClient).exec(ops).get.right.get.toVersionRecord.get
    }
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    val ops = scanamoTable.put(DynamoVersionRecord(record))

    Scanamo(dynamoClient).exec(ops) match {
      case Some(Left(err)) => throw new RuntimeException(s"Scanamo error: $err")
      case _               => ()
    }
  }
}
