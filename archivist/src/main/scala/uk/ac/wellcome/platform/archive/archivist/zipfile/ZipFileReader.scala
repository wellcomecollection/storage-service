package uk.ac.wellcome.platform.archive.archivist.zipfile

import java.io.InputStream

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.archivist.models.ZipEntryPointer

object ZipFileReader extends Logging {

  /** Returns an InputStream for a file inside a ZIP file.
    *
    * If something goes wrong (for example, the file doesn't exist),
    * it returns None rather than throwing an exception.
    *
    */
  def maybeInputStream(zipEntryPointer: ZipEntryPointer): Option[InputStream] = {
    val zipFile = zipEntryPointer.zipFile
    debug(s"Getting ZipEntryPointer ${zipEntryPointer.zipPath}")

    val maybeInputStream = for {
      zipEntry <- Option(zipFile.getEntry(zipEntryPointer.zipPath))
      zipStream <- Option(zipFile.getInputStream(zipEntry))
    } yield zipStream

    debug(s"MaybeInputStream: $maybeInputStream")
    maybeInputStream
  }
}
