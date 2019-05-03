package uk.ac.wellcome.platform.storage.bagauditor.services
import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

import scala.concurrent.Future

class VersionManager {
  def assignVersion(
    ingestId: IngestID,
    ingestDate: Instant,
    externalIdentifier: ExternalIdentifier
  ): Future[Int] = Future.successful(1)
}
