package uk.ac.wellcome.platform.archive.archivist.builders

import uk.ac.wellcome.platform.archive.archivist.models.BagItemLocation
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagItemPath,
  BagLocation
}
import uk.ac.wellcome.storage.ObjectLocation

object UploadLocationBuilder {
  def create(bagUploadLocation: BagLocation,
             itemPathInZip: String,
             maybeBagRootPathInZip: Option[String] = None): ObjectLocation = {
    val bagItemUploadLocation = BagItemLocation(
      bagLocation = bagUploadLocation,
      bagItemPath =
        BagItemPath(uploadItemPath(maybeBagRootPathInZip, itemPathInZip))
    )
    bagItemUploadLocation.objectLocation
  }

  private def uploadItemPath(maybeBagRootPathInZip: Option[String],
                             zipPath: String): String =
    maybeBagRootPathInZip match {
      case Some(bagRootPathInZip) => zipPath.stripPrefix(bagRootPathInZip)
      case None                   => zipPath
    }
}
