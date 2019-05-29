package uk.ac.wellcome.platform.storage.ingests.api

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.IngestRequestPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker

import scala.util.Try

class IngestStarter[UnpackerDestination](
  ingestTracker: IngestTracker,
  unpackerMessageSender: MessageSender[UnpackerDestination]
) {
  def initialise(ingest: Ingest): Try[Ingest] =
    for {
      ingest <- ingestTracker.initialise(ingest)
      _ <- unpackerMessageSender.sendT(IngestRequestPayload(ingest))
    } yield ingest
}
