package uk.ac.wellcome.platform.archive.indexer.bags.models

import java.time.{Instant, LocalDate}

import weco.storage_service.bagit.models.BagInfo
import weco.storage_service.storage.models.{
  StorageLocation,
  StorageManifest
}

case class IndexedBagInfo(
  externalIdentifier: String,
  payloadOxum: String,
  baggingDate: LocalDate,
  sourceOrganisation: Option[String] = None,
  externalDescription: Option[String] = None,
  internalSenderIdentifier: Option[String] = None,
  internalSenderDescription: Option[String] = None
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
  path: String
)

object IndexedLocation {
  def apply(location: StorageLocation): IndexedLocation =
    IndexedLocation(
      provider = location.provider.id,
      bucket = location.prefix.namespace,
      path = location.prefix.pathPrefix
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
  filesCount: Int,
  filesTotalSize: Long
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest = {
    val storageManifestFiles = storageManifest.manifest.files
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
      filesCount = storageManifestFiles.length,
      filesTotalSize = storageManifestFiles.map(_.size).sum
    )
  }
}
