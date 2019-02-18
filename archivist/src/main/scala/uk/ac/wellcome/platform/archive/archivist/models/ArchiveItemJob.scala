package uk.ac.wellcome.platform.archive.archivist.models

import uk.ac.wellcome.storage.ObjectLocation

trait ArchiveItemJob {
  val archiveJob: ArchiveJob
  val zipEntryPointer: ZipEntryPointer
  val uploadLocation: ObjectLocation
}

case class TagManifestItemJob(
  archiveJob: ArchiveJob,
  zipEntryPointer: ZipEntryPointer,
  uploadLocation: ObjectLocation
) extends ArchiveItemJob

case class DigestItemJob(
  archiveJob: ArchiveJob,
  zipEntryPointer: ZipEntryPointer,
  uploadLocation: ObjectLocation,
  digest: String
) extends ArchiveItemJob

