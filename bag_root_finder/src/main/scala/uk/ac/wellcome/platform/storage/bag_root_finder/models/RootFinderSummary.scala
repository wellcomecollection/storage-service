package uk.ac.wellcome.platform.storage.bag_root_finder.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait RootFinderSummary extends Summary {
  val location: ObjectLocationPrefix

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class RootFinderFailureSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant,
  location: ObjectLocationPrefix
) extends RootFinderSummary {
  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("location", location))
}

case class RootFinderSuccessSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant,
  location: ObjectLocationPrefix,
  rootLocation: ObjectLocationPrefix
) extends RootFinderSummary {
  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("location", location), ("root", rootLocation))
}
