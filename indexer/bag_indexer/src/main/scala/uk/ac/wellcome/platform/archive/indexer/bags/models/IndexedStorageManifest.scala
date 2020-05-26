package uk.ac.wellcome.platform.archive.indexer.bags.models

import java.time.Instant

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageManifestFile}
import uk.ac.wellcome.platform.archive.indexer.bags.services.FileSuffixCounter

case class IndexedSuffixTally(
  suffix: String,
  count: Int
)

object IndexedSuffixTally {
  def apply(tuple: (String, Int)):IndexedSuffixTally =
    IndexedSuffixTally(tuple._1, tuple._2)
}

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
  @JsonKey("type") ontologyType: String = "Bag"
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest = {
    val storageManifestFiles = storageManifest.manifest.files
    val payloadFiles = storageManifestFiles.map(IndexedFileFields(_))

    val fileSuffixTally = FileSuffixCounter.count(storageManifestFiles)
    val indexedSuffixTally = fileSuffixTally
      .map(IndexedSuffixTally(_))
      .toList.sortBy(_.suffix)

    val payloadStats = IndexedPayloadStats(
      fileCount = storageManifestFiles.length,
      fileSize = storageManifestFiles.map(_.size).sum,
      fileSuffixTally = indexedSuffixTally
    )

    IndexedStorageManifest(
      id = storageManifest.id.toString,
      space = storageManifest.space.underlying,
      version = storageManifest.version.underlying,
      createdDate = storageManifest.createdDate,
      payloadFiles = payloadFiles,
      payloadStats = payloadStats,
    )
  }
}


