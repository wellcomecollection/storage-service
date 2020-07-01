package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import org.apache.commons.io.FileUtils
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.{Location, Prefix}

case class UnpackSummary[SrcLocation <: Location, DstPrefix <: Prefix[_]](
  ingestId: IngestID,
  srcLocation: SrcLocation,
  dstPrefix: DstPrefix,
  fileCount: Long = 0L,
  bytesUnpacked: Long = 0L,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: UnpackSummary[SrcLocation, DstPrefix] =
    this.copy(maybeEndTime = Some(Instant.now()))

  def size: String =
    FileUtils.byteCountToDisplaySize(bytesUnpacked)

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(
      ("src", srcLocation),
      ("dst", dstPrefix),
      ("files", fileCount),
      ("bytesUnpacked", bytesUnpacked),
      ("size", size)
    )
}
