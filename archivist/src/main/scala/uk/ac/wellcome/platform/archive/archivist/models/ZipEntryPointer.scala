package uk.ac.wellcome.platform.archive.archivist.models

import java.util.zip.ZipFile

/** This represents an individual ZIP file and one of its entries,
  * as stored on disk.
  *
  * It _doesn't_ tell you where that entry should be uploaded to next.
  */
case class ZipEntryPointer(
  zipFile: ZipFile,
  zipPath: String
)
