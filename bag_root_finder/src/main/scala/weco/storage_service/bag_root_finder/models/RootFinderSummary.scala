package weco.storage_service.bag_root_finder.models

import java.time.Instant

import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary
import weco.storage.providers.s3.S3ObjectLocationPrefix

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
