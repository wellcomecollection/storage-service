package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerCreateError
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.{
  BadFetchLocationException,
  StorageManifestService
}

import scala.concurrent.{ExecutionContext, Future}

class Register(
  bagReader: BagReader[_],
  bagTrackerClient: BagTrackerClient,
  storageManifestService: StorageManifestService[_]
)(
  implicit ec: ExecutionContext
) extends Logging {

  def update(
    ingestId: IngestID,
    location: PrimaryStorageLocation,
    replicas: Seq[SecondaryStorageLocation],
    version: BagVersion,
    space: StorageSpace
  ): Future[IngestStepResult[RegistrationSummary]] = {

    val registration = RegistrationSummary(
      ingestId = ingestId,
      startTime = Instant.now(),
      location = location,
      space = space
    )

    val result: Future[IngestStepResult[RegistrationSummary]] = for {
      bag <- bagReader.get(location.prefix) match {
        case Right(value) => Future(value)
        case Left(err) =>
          Future.failed(
            new RuntimeException(s"Bag unavailable: ${err.msg}")
          )
      }

      storageManifest <- Future.fromTry {
        storageManifestService.createManifest(
          ingestId = ingestId,
          bag = bag,
          location = location,
          replicas = replicas,
          space = space,
          version = version
        )
      }

      completedRegistration <- bagTrackerClient.createBag(storageManifest)

      result = completedRegistration match {
        case Right(()) => IngestCompleted(registration.complete)
        case Left(BagTrackerCreateError(err)) =>
          error("Unexpected error updating storage manifest", err)
          IngestFailed(registration.complete, err)
      }
    } yield result

    result.recover {
      case err: BadFetchLocationException =>
        IngestFailed(
          registration.complete,
          err,
          maybeUserFacingMessage = Some(err.getMessage)
        )

      case err => IngestFailed(registration.complete, err)
    }
  }
}
