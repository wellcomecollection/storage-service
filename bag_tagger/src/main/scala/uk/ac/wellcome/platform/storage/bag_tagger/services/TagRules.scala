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
        case f if manifest.space == StorageSpace("digitised") && f.hasExtension(".mxf") =>
          f -> "application/mxf"
      }

    contentTypes
      .map { case (f, contentType) => f -> Map("Content-Type" -> contentType) }
      .toMap
  }

  implicit class FileOps(f: StorageManifestFile) {
    def hasExtension(ext: String): Boolean = {
      assert(ext.startsWith("."))
      assert(ext.toLowerCase == ext)
      f.path.toLowerCase.endsWith(ext )
    }
  }
}
