package uk.ac.wellcome.platform.storage.ingests.api.services

import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{IngestTrackerClient, IngestTrackerCreateError}

import scala.concurrent.{ExecutionContext, Future}

class IngestCreator[UnpackerDestination](
                     ingestTrackerClient: IngestTrackerClient,
                     unpackerMessageSender: MessageSender[UnpackerDestination]
                   )(implicit val executionContext: ExecutionContext) {
  import uk.ac.wellcome.json.JsonUtil._

  def create(ingest: Ingest): Future[Either[IngestTrackerCreateError, Unit]] = for {
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