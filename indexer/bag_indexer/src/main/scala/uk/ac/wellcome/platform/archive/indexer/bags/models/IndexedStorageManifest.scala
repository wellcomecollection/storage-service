package uk.ac.wellcome.platform.archive.indexer.bags.models

import java.time.Instant

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageManifestFile
}

case class IndexedSuffixTally(
  suffix: String,
  count: Int
)

case class IndexedPayloadStats(
  fileCount: Int,
  fileSize: Long,
  fileSuffixTally: Seq[IndexedSuffixTally]
)

case class IndexedFileFields(
  path: String,
  name: String,
  size: Long,
  checksum: String,
  @JsonKey("type") ontologyType: String = "File"
)

object IndexedFileFields {
  def apply(storageManifestFile: StorageManifestFile): IndexedFileFields = {
    IndexedFileFields(
      path = storageManifestFile.path,
      name = storageManifestFile.name,
      size = storageManifestFile.size,
      checksum = storageManifestFile.checksum.value
    )
  }
}

case class IndexedStorageManifest(
  id: String,
  space: String,
  version: Int,
  createdDate: Instant,
  payloadFiles: Seq[IndexedFileFields],
  payloadStats: IndexedPayloadStats,
  newPayloadStats: IndexedPayloadStats,
  @JsonKey("type") ontologyType: String = "Bag"
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest = {
    val payloadFiles = storageManifest.manifest.files.map(IndexedFileFields(_))

    val payloadStats = IndexedPayloadStats(
      fileCount = 0,
      fileSize = 0,
      fileSuffixTally = Nil
    )

    val newPayloadStats = IndexedPayloadStats(
      fileCount = 0,
      fileSize = 0,
      fileSuffixTally = Nil
    )

    IndexedStorageManifest(
      id = storageManifest.id.toString,
      space = storageManifest.space.underlying,
      version = storageManifest.version.underlying,
      createdDate = storageManifest.createdDate,
      payloadFiles = payloadFiles,
      payloadStats = payloadStats,
      newPayloadStats = newPayloadStats
    )
  }
}
