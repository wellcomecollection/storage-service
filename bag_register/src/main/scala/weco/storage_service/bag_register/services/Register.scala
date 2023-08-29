package weco.storage_service.bag_register.services

import java.time.Instant

import grizzled.slf4j.Logging
import weco.storage_service.bag_register.models.RegistrationSummary
import weco.storage_service.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerCreateError
}
import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models._
import weco.storage.RetryableError
import weco.storage.providers.s3.S3ObjectLocationPrefix

import scala.concurrent.{ExecutionContext, Future}

class Register(
  bagReader: S3BagReader,
  bagTrackerClient: BagTrackerClient,
  storageManifestService: S3StorageManifestService
)(
  implicit ec: ExecutionContext
) extends Logging {

  def update(
    ingestId: IngestID,
    location: PrimaryReplicaLocation,
    replicas: Seq[SecondaryReplicaLocation],
    version: BagVersion,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier
  ): Future[IngestStepResult[RegistrationSummary]] = {

    val registration = RegistrationSummary(
      ingestId = ingestId,
      startTime = Instant.now(),
      location = location,
      space = space,
      externalIdentifier = externalIdentifier,
      version = version
    )

    val result: Future[IngestStepResult[RegistrationSummary]] = for {
      bag <- bagReader.get(location.prefix.asInstanceOf[S3ObjectLocationPrefix]) match {
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

        case Left(createError: BagTrackerCreateError)
            if createError.isInstanceOf[RetryableError] =>
          warn(s"Retryable error updating storage manifest: ${createError.err}")
          IngestShouldRetry(registration, e = createError.err)

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
