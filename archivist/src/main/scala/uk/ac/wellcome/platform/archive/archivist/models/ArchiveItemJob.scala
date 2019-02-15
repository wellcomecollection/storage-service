package uk.ac.wellcome.platform.archive.archivist.models

import uk.ac.wellcome.platform.archive.common.models.bagit.{BagItemLocation, BagItemPath, BagLocation}
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

object UploadLocationBuilder {
  def create(bagUploadLocation: BagLocation,
             itemPathInZip: String,
             maybeBagRootPathInZip: Option[String] = None): ObjectLocation = {
    val bagItemUploadLocation = BagItemLocation(
      bagLocation = bagUploadLocation,
      bagItemPath = BagItemPath(uploadItemPath(maybeBagRootPathInZip, itemPathInZip))
    )
    bagItemUploadLocation.objectLocation
  }

  private def uploadItemPath(maybeBagRootPathInZip: Option[String], zipPath: String): String =
    maybeBagRootPathInZip match {
      case Some (bagRootPathInZip) => stripPrefix(bagRootPathInZip, zipPath)
      case None => zipPath
    }

  private def stripPrefix(prefix: String, path: String): String = {
    if (path.startsWith(prefix))
      path.substring(prefix.length)
    else
      path
  }
}
