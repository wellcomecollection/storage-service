package uk.ac.wellcome.platform.archive.common.operation.models

trait IdentifiedSummary extends Summary with Identified {
  override def toString: String = {
    f"""|id=$id
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
