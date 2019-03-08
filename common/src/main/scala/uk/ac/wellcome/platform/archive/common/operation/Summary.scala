package uk.ac.wellcome.platform.archive.common.operation

trait Summary extends Timed {
  override def toString: String = {
    f"""|completed in $formatDuration
        |"""
      .stripMargin
      .replaceAll("\n", " ")
  }

  def formatDuration: String =
    duration.getOrElse("<not-completed>").toString
}
