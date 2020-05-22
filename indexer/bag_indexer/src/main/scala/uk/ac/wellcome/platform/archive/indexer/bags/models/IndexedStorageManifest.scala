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
  payloadFileCount: Int,
  payloadFileSize: Long,
  payloadFileSuffixTally: Seq[IndexedSuffixTally],
  newPayloadFileCount: Int,
  newPayloadFileSize: Long,
  newPayloadFileSuffixTally: Seq[IndexedSuffixTally],
  @JsonKey("type") ontologyType: String = "Bag"
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest = {
    val payloadFiles = storageManifest.manifest.files.map(IndexedFileFields(_))

    IndexedStorageManifest(
      id = storageManifest.id.toString,
      space = storageManifest.space.underlying,
      version = storageManifest.version.underlying,
      createdDate = storageManifest.createdDate,
      payloadFiles = payloadFiles,
      payloadFileCount = 0,
      payloadFileSize = 0,
      payloadFileSuffixTally = Nil,
      newPayloadFileCount = 0,
      newPayloadFileSize = 0,
      newPayloadFileSuffixTally = Nil
    )
  }
}
