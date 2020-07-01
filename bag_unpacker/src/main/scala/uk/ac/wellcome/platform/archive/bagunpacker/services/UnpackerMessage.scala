package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.text.NumberFormat

import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary

object UnpackerMessage {

  // Creates the human-readable message for the ingest status, e.g.
  //
  //    Unpacked 50MB from 30 files
  //
  def create(summary: UnpackSummary): String = {
    val displayFileCount = NumberFormat.getInstance().format(summary.fileCount)
    s"Unpacked ${summary.size} from $displayFileCount file${if (summary.fileCount != 1) "s"
    else ""}"
  }
}
