package uk.ac.wellcome.platform.archive.common.operation.models

import org.apache.commons.io.FileUtils

trait Summary extends Timed {
  override def toString: String = {
    f"""|duration=$formatDuration"""
      .stripMargin
      .replaceAll("\n", ", ")
  }

  def formatDuration: String =
    duration.getOrElse("<not-completed>").toString

  def formatBytes(bytes: Long) = {
    FileUtils.byteCountToDisplaySize(bytes)
  }
}
