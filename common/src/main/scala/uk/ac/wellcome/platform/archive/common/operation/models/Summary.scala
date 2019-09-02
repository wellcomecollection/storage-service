package uk.ac.wellcome.platform.archive.common.operation.models

import org.apache.commons.io.FileUtils
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID

trait Summary extends Timed {
  val ingestId: IngestID

  val fieldsToLog: Seq[(String, Any)]

  override def toString: String =
    (fieldsToLog ++ Seq(
      ("ingestId", ingestId),
      ("duration", formatDuration),
      ("durationSeconds", durationSeconds)
    ))
      .map { case (key, value) => s"$key=$value" }
      .mkString(", ")

  private def formatDuration: String =
    duration.getOrElse("<not-completed>").toString

  def formatBytes(bytes: Long): String =
    FileUtils.byteCountToDisplaySize(bytes)
}
