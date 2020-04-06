package uk.ac.wellcome.platform.archive.bag_indexer.models

import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageManifestFile
}

case class IndexedChecksum(
  algorithm: String,
  value: String
)

case class BagPointer(
  space: String,
  externalIdentifier: String,
  versions: Seq[String]
)

case class IndexedFile(
  bucket: String,
  path: String,
  name: String,
  size: Long,
  createdDate: String,
  checksum: IndexedChecksum,
  bag: BagPointer
) {
  def id: String =
    s"s3://$bucket/$path"
}

case object IndexedFile {
  def apply(manifest: StorageManifest, file: StorageManifestFile): IndexedFile =
    IndexedFile(
      bucket = manifest.location.prefix.namespace,
      path = manifest.location.prefix.asLocation(file.path).path,
      name = file.name,
      size = file.size,
      createdDate = manifest.createdDate.toString,
      checksum = IndexedChecksum(
        algorithm = manifest.manifest.checksumAlgorithm.toString,
        value = file.checksum.toString
      ),
      bag = BagPointer(
        space = manifest.space.toString,
        externalIdentifier = manifest.info.externalIdentifier.toString,
        versions = Seq(manifest.version.toString)
      )
    )
}
