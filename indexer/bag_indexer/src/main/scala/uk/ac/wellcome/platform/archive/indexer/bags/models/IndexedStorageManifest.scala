package uk.ac.wellcome.platform.archive.indexer.bags.models

import java.time.{Instant, LocalDate}

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.BagInfo
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageLocation,
  StorageManifest,
  StorageManifestFile
}
import uk.ac.wellcome.platform.archive.indexer.bags.services.FileSuffix

case class IndexedFileFields(
  path: String,
  name: String,
  suffix: Option[String],
  size: Long,
  checksum: String,
  @JsonKey("type") ontologyType: String = "File"
)

object IndexedFileFields {
  def apply(file: StorageManifestFile): IndexedFileFields = {
    IndexedFileFields(
      path = file.path,
      name = file.name,
      suffix = FileSuffix.getSuffix(file.name),
      size = file.size,
      checksum = file.checksum.value
    )
  }
}

case class IndexedBagInfo(
  externalIdentifier: String,
  payloadOxum: String,
  baggingDate: LocalDate,
  sourceOrganisation: Option[String] = None,
  externalDescription: Option[String] = None,
  internalSenderIdentifier: Option[String] = None,
  internalSenderDescription: Option[String] = None,
  @JsonKey("type") ontologyType: String = "BagInfo"
)

object IndexedBagInfo {
  def apply(info: BagInfo): IndexedBagInfo =
    IndexedBagInfo(
      externalIdentifier = info.externalIdentifier.toString,
      payloadOxum = info.payloadOxum.toString,
      baggingDate = info.baggingDate,
      sourceOrganisation = info.sourceOrganisation.map(_.toString),
      externalDescription = info.externalDescription.map(_.toString),
      internalSenderIdentifier = info.internalSenderIdentifier.map(_.toString),
      internalSenderDescription = info.internalSenderDescription.map(_.toString)
    )
}

case class IndexedLocation(
  provider: String,
  bucket: String,
  path: String,
  @JsonKey("type") ontologyType: String = "Location"
)

object IndexedLocation {
  def apply(location: StorageLocation): IndexedLocation =
    IndexedLocation(
      provider = location.provider.toString,
      bucket = location.prefix.namespace,
      path = location.prefix.path
    )
}

case class IndexedStorageManifest(
  id: String,
  space: String,
  version: Int,
  createdDate: Instant,
  info: IndexedBagInfo,
  location: IndexedLocation,
  replicaLocations: Seq[IndexedLocation],
  files: Seq[IndexedFileFields],
  filesCount: Int,
  filesTotalSize: Long,
  @JsonKey("type") ontologyType: String = "Bag"
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest = {
    val storageManifestFiles = storageManifest.manifest.files
    val payloadFiles = storageManifestFiles.map(IndexedFileFields(_))
    val replicaLocations =
      storageManifest.replicaLocations.map(IndexedLocation(_))

    IndexedStorageManifest(
      id = storageManifest.id.toString,
      space = storageManifest.space.underlying,
      version = storageManifest.version.underlying,
      createdDate = storageManifest.createdDate,
      info = IndexedBagInfo(storageManifest.info),
      location = IndexedLocation(storageManifest.location),
      replicaLocations = replicaLocations,
      files = payloadFiles,
      filesCount = storageManifestFiles.length,
      filesTotalSize = storageManifestFiles.map(_.size).sum
    )
  }
}
