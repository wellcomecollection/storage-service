package uk.ac.wellcome.platform.storage.bag_tagger.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

/** Given a storage manifest, decide what tags (if any) should be applied to the
  * objects in the bag.
  *
  */
object TagRules {
  def chooseTags(manifest: StorageManifest): Map[BagPath, Map[String, String]] =
    contentTypeForMxfMasters(manifest)

  // We keep high-resolution MXF masters in the digitised space.
  //
  // We keep them around in case we need to reencode the access copies in
  // future, but we don't need immediate access to them.  We apply a tag
  // so they can be lifecycled to a cold storage tier.
  private def contentTypeForMxfMasters(manifest: StorageManifest): Map[BagPath, Map[String, String]] =
    manifest.space.underlying match {
      case "digitised" =>
        manifest.manifest.files
          .filter { _.path.toLowerCase.endsWith(".mxf") }
          .map { manifestFile => BagPath(manifestFile.name) -> Map("Content-Type" -> "application/mxf") }
          .toMap

      case _ => Map.empty
    }
}
