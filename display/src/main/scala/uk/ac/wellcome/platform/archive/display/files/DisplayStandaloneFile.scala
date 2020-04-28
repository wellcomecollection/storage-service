package uk.ac.wellcome.platform.archive.display.files

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageManifestFile
}

// We index individual files into their own index in Elasticsearch.
//
// For example, somebody might want to query for all the JP2 files created in
// the "digitised" space in a given month.
//
// We don't want to ingest just the DisplayFile model used in the bags API,
// because that doesn't have any context about the space of external identifier
// of this particular bag.

case class DisplayAssociatedBag(
  space: String,
  externalIdentifier: String,
  version: String,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "AssociatedBag"
)

case object DisplayAssociatedBag {
  def apply(storageManifest: StorageManifest): DisplayAssociatedBag =
    DisplayAssociatedBag(
      space = storageManifest.space.underlying,
      externalIdentifier = storageManifest.info.externalIdentifier.underlying,
      version = storageManifest.version.toString,
      createdDate = storageManifest.createdDate.toString
    )
}

case class DisplayStandaloneFile(
  checksum: String,
  name: String,
  path: String,
  size: Long,
  bag: DisplayAssociatedBag,
  @JsonKey("type") ontologyType: String = "File"
)

case object DisplayStandaloneFile {
  def apply(
    file: StorageManifestFile,
    storageManifest: StorageManifest
  ): DisplayStandaloneFile = {
    require(
      storageManifest.manifest.files.contains(file),
      s"File ${file.name} is not part of storage manifest ${storageManifest.idWithVersion}"
    )

    DisplayStandaloneFile(
      checksum =
        s"${storageManifest.manifest.checksumAlgorithm.pathRepr}:${file.checksum.value}",
      name = file.name,
      path = file.path,
      size = file.size,
      bag = DisplayAssociatedBag(storageManifest)
    )
  }
}
