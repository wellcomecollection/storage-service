package uk.ac.wellcome.platform.archive.common.storage.services.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import org.scanamo.auto._
import org.scanamo.{Scanamo, Table => ScanamoTable}
import org.scanamo.syntax._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.{
  ObjectLocation,
  ObjectLocationPrefix,
  ReadError,
  StoreReadError
}
import uk.ac.wellcome.storage.dynamo.{DynamoConfig, DynamoHashRangeEntry}
import uk.ac.wellcome.storage.s3.S3Config
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.{
  DynamoHashRangeStore,
  DynamoHybridStoreWithMaxima,
  DynamoVersionedHybridStore
}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.streaming.Codec._

import scala.util.{Failure, Success, Try}

class DynamoStorageManifestDao(
  dynamoConfig: DynamoConfig,
  s3Config: S3Config
)(
  implicit
  dynamoClient: AmazonDynamoDB,
  s3Client: AmazonS3
) extends StorageManifestDao {

  implicit val indexedStore: DynamoHashRangeStore[BagId, Int, ObjectLocation] =
    new DynamoHashRangeStore[BagId, Int, ObjectLocation](dynamoConfig)

  implicit val streamStore: S3StreamStore = new S3StreamStore()
  implicit val typedStore: S3TypedStore[StorageManifest] =
    new S3TypedStore[StorageManifest]()

  override val vhs: VersionedStore[BagId, Int, StorageManifest] =
    new DynamoVersionedHybridStore[BagId, Int, StorageManifest](
      store = new DynamoHybridStoreWithMaxima[
        BagId,
        Int,
        StorageManifest
      ](
        prefix = ObjectLocationPrefix(
          namespace = s3Config.bucketName,
          path = ""
        )
      )
    )

  override def listVersions(
    bagId: BagId,
    before: Option[BagVersion]
  ): Either[ReadError, Seq[StorageManifest]] =
    for {
      s3Locations <- getDynamoIndexEntries(
        bagId = bagId,
        before = before
      )

      manifests <- getManifests(
        bagId = bagId,
        before = before,
        locations = s3Locations
      )
    } yield manifests

  private def getManifests(
    bagId: BagId,
    before: Option[BagVersion],
    locations: Seq[ObjectLocation]
  ): Either[StoreReadError, Seq[StorageManifest]] = {
    val s3Results = locations.map { typedStore.get }

    val successes = s3Results.collect {
      case Right(entry) => entry.identifiedT
    }
    val errors = s3Results.collect { case Left(err) => err }

    Either.cond(
      errors.isEmpty,
      right = successes,
      left = StoreReadError(
        new Throwable(
          s"Errors fetching S3 objects for manifests $bagId before=$before: $errors"
        )
      )
    )
  }

  private def getDynamoIndexEntries(
    bagId: BagId,
    before: Option[BagVersion]
  ): Either[StoreReadError, Seq[ObjectLocation]] = {
    val table = ScanamoTable[DynamoHashRangeEntry[
      BagId,
      Int,
      ObjectLocation
    ]](dynamoConfig.tableName)

    val baseOps = before match {
      case Some(beforeVersion) =>
        table.descending.from('id -> bagId and 'version -> beforeVersion)
      case None => table.descending
    }

    val ops = baseOps.query('id -> bagId)

    for {
      scanamoResult <- Try {
        Scanamo(dynamoClient).exec(ops)
      } match {
        case Failure(err)   => Left(StoreReadError(err))
        case Success(value) => Right(value)
      }

      scanamoSuccesses = scanamoResult.collect { case Right(entry) => entry }
      scanamoErrors = scanamoResult.collect { case Left(err)       => err }

      indexEntries <- Either.cond(
        scanamoErrors.isEmpty,
        right = scanamoSuccesses,
        left = StoreReadError(
          new Throwable(
            s"Errors querying DynamoDB for $bagId before=$before: $scanamoErrors"
          )
        )
      )
    } yield indexEntries.map { _.payload }
  }
}
