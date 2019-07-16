package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import org.scanamo.auto._
import org.scanamo.{DynamoFormat, DynamoValue}
import org.scanamo.error.DynamoReadError
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.s3.S3Config
import uk.ac.wellcome.storage.store.dynamo.{DynamoHashRangeStore, DynamoHybridStoreWithMaxima, DynamoVersionedHybridStore}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreEntry, VersionedStore}
import uk.ac.wellcome.storage.streaming.Codec._

case class EmptyMetadata()

trait BetterStorageManifestDao {
  val vhs: VersionedStore[BagId, Int, HybridStoreEntry[StorageManifest, EmptyMetadata]]
  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id).map { _.identifiedT.t }

  def get(id: BagId, version: Int): Either[ReadError, StorageManifest] =
    vhs.get(Version(id, version)).map { _.identifiedT.t }

  def put(storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .put(id = Version(storageManifest.id, storageManifest.version))(
        HybridStoreEntry(storageManifest, metadata = EmptyMetadata())
      )
      .map { _.identifiedT.t }
}

class MemoryStorageManifestDao(
  val vhs: MemoryVersionedStore[BagId, HybridStoreEntry[StorageManifest, EmptyMetadata]]
) extends BetterStorageManifestDao

class DynamoStorageManifestDao(
  dynamoConfig: DynamoConfig,
  s3Config: S3Config
)(
  implicit
  dynamoClient: AmazonDynamoDB,
  s3Client: AmazonS3
) extends BetterStorageManifestDao {
  type DynamoStoreEntry =
    HybridIndexedStoreEntry[ObjectLocation, EmptyMetadata]

  implicit val evidence: DynamoFormat[EmptyMetadata] = new DynamoFormat[EmptyMetadata] {
    override def read(av: DynamoValue): scala.Either[DynamoReadError, EmptyMetadata] =
      Right(EmptyMetadata())

    override def write(t: EmptyMetadata): DynamoValue =
      DynamoValue.nil
  }

  implicit val indexedStore: DynamoHashRangeStore[BagId, Int, DynamoStoreEntry] =
    new DynamoHashRangeStore[BagId, Int, DynamoStoreEntry](dynamoConfig)

  implicit val streamStore: S3StreamStore = new S3StreamStore()
  implicit val typedStore: S3TypedStore[StorageManifest] =
    new S3TypedStore[StorageManifest]()

  override val vhs: VersionedStore[BagId, Int, HybridStoreEntry[StorageManifest, EmptyMetadata]] =
    new DynamoVersionedHybridStore[
      BagId, Int, StorageManifest, EmptyMetadata](
      store = new DynamoHybridStoreWithMaxima[
        BagId, Int, StorageManifest, EmptyMetadata](
        prefix = ObjectLocationPrefix(
          namespace = s3Config.bucketName,
          path = ""
        )
      )
    )
}

// TODO: Do we need this wrapper at all now?
// TODO: This could be a Store!
class StorageManifestDao(
  val vhs: VersionedStore[
    String,
    Int,
    HybridStoreEntry[StorageManifest, Map[String, String]]]
) {
  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id.toString).map { _.identifiedT.t }

  def get(id: BagId, version: Int): Either[ReadError, StorageManifest] =
    vhs.get(Version(id.toString, version)).map { _.identifiedT.t }

  def put(
    storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .put(id = Version(storageManifest.id.toString, storageManifest.version))(
        HybridStoreEntry(storageManifest, metadata = Map("alex" -> "true"))
      )
      .map { _.identifiedT.t }
}
