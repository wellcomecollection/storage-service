package uk.ac.wellcome.platform.storage.bag_tagger.services

import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageManifestFile,
  StorageSpace
}

/** Given a storage manifest, decide what tags (if any) should be applied to the
  * objects in the bag.
  *
  */
object TagRules {
  def chooseTags(
    manifest: StorageManifest
  ): Map[StorageManifestFile, Map[String, String]] =
    createContentTypeTags(manifest)

  /** Create tags of the form Map("Content-Type" -> [MIME type])
    *
    * Our tags are based on the official list of MIME types from
    * http://www.iana.org/assignments/media-types/media-types.xhtml
    *
    * We prefer to tag with MIME types because these are standardised tags
    * that might be useful elsewhere, rather than inventing an unnecessary
    * tagging scheme solely for the storage service.
    */
  private def createContentTypeTags(
    manifest: StorageManifest
  ): Map[StorageManifestFile, Map[String, String]] = {
    val contentTypes =
      manifest.manifest.files.collect {

        // In our digitised A/V workflow, we keep both a high-resolution MXF
        // and a lower-resolution MP4.
        //
        // We apply a Content-Type tag so the MXF can be lifecycled to a
        // cold storage tier, because we don't need immediate access to it.
        // Services like DLCS will use the MP4 to serve videos on the web.
        case f
            if manifest.space == StorageSpace("digitised") && f.hasExtension(
              ".mxf"
            ) =>
          f -> "application/mxf"

        // In our digitised manuscripts workflow, we keep both the original TIFF
        // and the edited JP2 from LayoutWizzard.
        //
        // We apply a Content-Type tag so the TIFF can be lifecycled to a
        // cold storage tier, because DLCS will use the JP2 to serve images on the web.
        case f
            if manifest.space == StorageSpace("digitised") && f.hasExtension(
              ".tif",
              ".tiff"
            ) =>
          f -> "image/tiff"
      }

    contentTypes.map {
      case (f, contentType) => f -> Map("Content-Type" -> contentType)
    }.toMap
  }

  implicit class FileOps(f: StorageManifestFile) {
    def hasExtension(exts: String*): Boolean = {
      exts.foreach { e =>
        assert(e.startsWith("."))
        assert(e.toLowerCase == e)
      }

      exts.exists(f.path.toLowerCase.endsWith)
    }
  }
}
