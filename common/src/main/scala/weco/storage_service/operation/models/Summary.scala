package weco.storage_service.operation.models

import weco.storage_service.ingests.models.IngestID

trait Summary extends Timed {
  val ingestId: IngestID

  val fieldsToLog: Seq[(String, Any)]

  override def toString: String =
    (fieldsToLog ++ Seq(
      ("ingestId", ingestId),
      ("duration", formatDuration),
      ("durationSeconds", durationSeconds)
    )).map { case (key, value) => s"$key=$value" }
      .mkString(", ")

  private def formatDuration: String =
    duration.getOrElse("<not-completed>").toString
}
