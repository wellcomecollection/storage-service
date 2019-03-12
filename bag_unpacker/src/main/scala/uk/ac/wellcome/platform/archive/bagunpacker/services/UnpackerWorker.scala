package uk.ac.wellcome.platform.archive.bagunpacker.services

import scala.concurrent.{ExecutionContext, Future}

class UnpackerWorker(
  config: UnpackerConfig,
  stream: NotificationStream[UnpackBagRequest],
  ingestUpdater: IngestUpdater,
  outgoing: OutgoingPublisher,
  reporter: DiagnosticReporter,
  unpacker: Unpacker,
)(implicit ec: ExecutionContext)
    extends Logging
    with Runnable {

  def run(): Future[Done] = stream.run(processMessage)

  def processMessage(request: UnpackBagRequest)(
    implicit enc: Encoder[BagRequest]): Future[Unit] = {
    val location = BagLocationBuilder.build(request, config)

    for {
      result <- unpacker.unpack(
        request.sourceLocation,
        location.objectLocation
      )

      _ <- reporter.report(request.requestId, result)

      _ <- ingestUpdater.send(request.requestId, result)
      _ <- outgoing.send(request.requestId, result)(_ =>
        BagRequest(request.requestId, location))

    } yield ()
  }
}
