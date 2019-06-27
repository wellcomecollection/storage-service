package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.auto._
import org.scanamo.syntax._
import org.scanamo.{DynamoFormat, Scanamo, Table => ScanamoTable}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
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

  private val table = ScanamoTable[DynamoEntry](dynamoConfig.tableName)
  private val index = table.index(dynamoConfig.indexName)

  val scanamoTable = ScanamoTable[DynamoEntry](dynamoConfig.tableName)

  val maxima = new DynamoHashRangeMaxima[String, Int, DynamoEntry] {
    override protected implicit val formatHashKey: DynamoFormat[String] = DynamoFormat[String]
    override protected implicit val formatRangeKey: DynamoFormat[Int] = DynamoFormat[Int]
    override protected implicit val format: DynamoFormat[DynamoEntry] = DynamoFormat[DynamoEntry]
    override protected val client: AmazonDynamoDB = dynamoClient
    override protected val table: ScanamoTable[DynamoEntry] = scanamoTable
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
    storageSpace: StorageSpace): Either[MaximaError, Int] = {
    val id = DynamoID.createId(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier
    )

    maxima.max(id)
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    val ops = scanamoTable.put(DynamoEntry(record))

    Scanamo(dynamoClient).exec(ops) match {
      case Some(Left(err)) => throw new RuntimeException(s"Scanamo error: $err")
      case _               => ()
    }
  }
}
