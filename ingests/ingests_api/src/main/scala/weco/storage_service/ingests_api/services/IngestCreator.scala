package weco.storage_service.ingests_api.services

import weco.messaging.MessageSender
import weco.storage_service.SourceLocationPayload
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.ingests_tracker.client.{
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
