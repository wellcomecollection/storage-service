package uk.ac.wellcome.platform.archive.common.operation

trait Summary extends Timed {
  override def toString: String = {
    f"""|completed in $describeDuration
        |"""
      .stripMargin
      .replaceAll("\n", " ")
  }

  def describeDuration: String =
    duration.getOrElse("<not-completed>").toString
}
