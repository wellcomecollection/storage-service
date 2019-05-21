package uk.ac.wellcome.platform.archive.common.ingests.monitor

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagIngest,
  Ingest,
  IngestUpdate
}

import scala.util.Try

trait IngestTracker {
  def get(id: IngestID): Try[Option[Ingest]]

  def initialise(ingest: Ingest): Try[Ingest]

  def update(update: IngestUpdate): Try[Ingest]

  def findByBagId(bagId: BagId): Try[Seq[BagIngest]]
}
