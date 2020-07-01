package uk.ac.wellcome.platform.storage.bag_root_finder.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.S3ObjectLocationPrefix

sealed trait RootFinderSummary extends Summary {
  val searchRoot: S3ObjectLocationPrefix

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class RootFinderFailureSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant,
  searchRoot: S3ObjectLocationPrefix
) extends RootFinderSummary {
  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("prefix", searchRoot))
}

case class RootFinderSuccessSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant,
  searchRoot: S3ObjectLocationPrefix,
  bagRoot: S3ObjectLocationPrefix
) extends RootFinderSummary {
  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("searchRoot", searchRoot), ("bagRoot", bagRoot))
}
