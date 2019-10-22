package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import org.apache.commons.io.FileUtils
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

case class UnpackSummary(
  ingestId: IngestID,
  srcLocation: ObjectLocation,
  dstLocation: ObjectLocationPrefix,
  fileCount: Long = 0L,
  bytesUnpacked: Long = 0L,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: UnpackSummary =
    this.copy(maybeEndTime = Some(Instant.now()))

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(
      ("src", srcLocation),
      ("dst", dstLocation),
      ("files", fileCount),
      ("bytesUnpacked", bytesUnpacked),
      ("size", FileUtils.byteCountToDisplaySize(bytesUnpacked))
    )
}
