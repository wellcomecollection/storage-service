package weco.storage_service.bag_tracker.storage.dynamo

import org.scanamo.generic.auto._
import org.scanamo.{Scanamo, Table => ScanamoTable}
import org.scanamo.syntax._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.transfer.s3.S3TransferManager
import weco.json.JsonUtil._
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.BagVersion._
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.storage.models.StorageManifest
import weco.storage.dynamo.{DynamoConfig, DynamoHashRangeEntry}
import weco.storage.s3.{S3Config, S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.VersionedStore
import weco.storage.store.dynamo.{
  ConsistencyMode,
  DynamoHashRangeStore,
  DynamoHybridStoreWithMaxima,
  DynamoVersionedHybridStore,
  StronglyConsistent
}
import weco.storage.store.s3.S3TypedStore
import weco.storage.streaming.Codec._
import weco.storage.{ReadError, StoreReadError}

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

class DynamoStorageManifestDao(
  dynamoConfig: DynamoConfig,
  s3Config: S3Config
)(
  implicit
  dynamoClient: DynamoDbClient,
  s3Client: S3Client,
  s3TransferManager: S3TransferManager
) extends StorageManifestDao {

  //  By default reads are eventually consistent
  //  See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
  //  The StronglyConsistent mode ensures that we always read the most recent data
  implicit val consistencyMode: ConsistencyMode =
    StronglyConsistent

  implicit val indexedStore
    : DynamoHashRangeStore[BagId, Int, S3ObjectLocation] =
    new DynamoHashRangeStore[BagId, Int, S3ObjectLocation](dynamoConfig)

  implicit val typedStore: S3TypedStore[StorageManifest] =
    S3TypedStore[StorageManifest]

  override val vhs: VersionedStore[BagId, Int, StorageManifest] =
    new DynamoVersionedHybridStore[BagId, Int, StorageManifest](
      store = new DynamoHybridStoreWithMaxima[
        BagId,
        Int,
        StorageManifest
      ](
        prefix = S3ObjectLocationPrefix(
          bucket = s3Config.bucketName,
          keyPrefix = ""
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
    locations: Seq[S3ObjectLocation]
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
  ): Either[StoreReadError, Seq[S3ObjectLocation]] = {
    val table = ScanamoTable[DynamoHashRangeEntry[
      BagId,
      Int,
      S3ObjectLocation
    ]](dynamoConfig.tableName)

    val baseOps = before match {
      case Some(beforeVersion) =>
        table.descending.from("id" === bagId and "version" === beforeVersion)
      case None => table.descending
    }

    val ops = baseOps.query("id" === bagId)

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
