package uk.ac.wellcome.platform.storage.ingests.api

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker

import scala.util.{Failure, Success, Try}

class IngestStarter[UnpackerDestination](
  ingestTracker: IngestTracker,
  unpackerMessageSender: MessageSender[UnpackerDestination]
) {
  def initialise(ingest: Ingest): Try[Ingest] =
    for {
      ingest <- ingestTracker.init(ingest) match {
        case Right(result) => Success(result.identifiedT)
        case Left(err) =>
          Failure(new Throwable(s"Error form the ingest tracker: $err"))
      }
      _ <- unpackerMessageSender.sendT(SourceLocationPayload(ingest))
    } yield ingest
}
