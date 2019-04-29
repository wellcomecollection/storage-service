package uk.ac.wellcome.platform.storage.ingests.api

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

import scala.concurrent.{ExecutionContext, Future}

class IngestStarter(
  ingestTracker: IngestTracker,
  unpackerSnsWriter: SNSWriter
)(implicit ec: ExecutionContext) {
  def initialise(ingest: Ingest): Future[Ingest] =
    for {
      ingest <- ingestTracker.initialise(ingest)
      _ <- unpackerSnsWriter.writeMessage(
        toObjectLocationPayload(ingest),
        subject = "ingest-created"
      )
    } yield ingest

  private def toObjectLocationPayload(
    ingest: Ingest
  ): ObjectLocationPayload =
    ObjectLocationPayload(
      ingestId = ingest.id,
      storageSpace = StorageSpace(ingest.space.underlying),
      objectLocation = ingest.sourceLocation.location
    )
}
