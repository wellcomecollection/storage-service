package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.storage.StorageManifestVHS
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.LocalVersionedHybridStore
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.vhs.EmptyMetadata

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait StorageManifestVHSFixture extends LocalVersionedHybridStore {
  def withStorageManifestVHS[R](table: Table, bucket: Bucket)(
    testWith: TestWith[StorageManifestVHS, R]): R =
    withTypeVHS[StorageManifest, EmptyMetadata, R](bucket, table) { vhs =>
      val storageManifestVHS = new StorageManifestVHS(vhs = vhs)
      testWith(storageManifestVHS)
    }

  def storeSingleManifest(
    vhs: StorageManifestVHS,
    storageManifest: StorageManifest): Future[Unit] =
    vhs.updateRecord(
      ifNotExisting = storageManifest
    )(
      ifExisting = _ => throw new RuntimeException("VHS should be empty!")
    )

  def getStorageManifest(table: Table, id: BagId): StorageManifest = {
    val hybridRecord = getHybridRecord(table, id.toString)
    getObjectFromS3[StorageManifest](hybridRecord.location)
  }
}
