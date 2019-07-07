package uk.ac.wellcome.platform.archive.bagunpacker.storage

import org.apache.commons.compress.archivers.ArchiveException
import org.apache.commons.compress.compressors.CompressorException

sealed trait ArchiveError {
  val e: Throwable
}

case class CompressorError(e: CompressorException) extends ArchiveError
case class ArchiveFormatError(e: ArchiveException) extends ArchiveError
case class UnexpectedArchiveError(e: Throwable) extends ArchiveError
