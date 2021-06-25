package uk.ac.wellcome.platform.storage.ingests.api.services

import weco.messaging.MessageSender
import weco.storage.SourceLocationPayload
import weco.storage_service.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerCreateError
}

import scala.concurrent.{ExecutionContext, Future}

class IngestCreator[UnpackerDestination](
  ingestTrackerClient: IngestTrackerClient,
  unpackerMessageSender: MessageSender[UnpackerDestination]
)(implicit val executionContext: ExecutionContext) {
  import weco.json.JsonUtil._

  def create(ingest: Ingest): Future[Either[IngestTrackerCreateError, Unit]] =
    for {
      trackerResult <- ingestTrackerClient.createIngest(ingest)
      _ <- trackerResult match {
        case Right(_) =>
          Future.fromTry {
            unpackerMessageSender.sendT(SourceLocationPayload(ingest))
          }
        case Left(_) => Future.successful(())
      }
    } yield trackerResult
}
