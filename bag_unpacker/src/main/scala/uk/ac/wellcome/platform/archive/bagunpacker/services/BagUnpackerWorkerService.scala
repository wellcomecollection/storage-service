package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.nio.file.Paths
import java.util.UUID

import akka.Done
import grizzled.slf4j.Logging
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagunpacker.BagUnpackerConfig
import uk.ac.wellcome.platform.archive.common.models.UnpackBagRequest
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagUnpackerWorkerService(
  config: BagUnpackerConfig,
  stream: NotificationStream[UnpackBagRequest],
  notificationService: NotificationService,
  unpackerService: UnpackerService
) extends Logging
    with Runnable {

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(
    unpackBagRequest: UnpackBagRequest
  ): Future[Unit] = {

    val destinationLocation = UnpackDestination(
      config.namespace,
      config.prefix,
      unpackBagRequest.requestId
    )

    for {
      unpackResult <- unpackerService.unpack(
        unpackBagRequest.sourceLocation, destinationLocation
      )

      _ = info(s"Unpacked bag: $unpackResult")

      _ <- notificationService.sendOutgoingNotification(unpackBagRequest)
    } yield unpackResult
  }
}

object UnpackDestination {
  def apply(namespace: String, prefix: String, id: UUID) = {
    ObjectLocation(
      namespace = namespace,
      key = Paths.get(prefix, id.toString).toString
    )
  }
}