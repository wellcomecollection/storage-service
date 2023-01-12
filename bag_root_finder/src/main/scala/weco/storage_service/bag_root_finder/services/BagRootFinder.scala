package weco.storage_service.bag_root_finder.services

import java.time.Instant
import software.amazon.awssdk.services.s3.S3Client
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import weco.storage_service.bag_root_finder.models._
import weco.storage.s3.S3ObjectLocationPrefix

import scala.util.{Failure, Success, Try}

class BagRootFinder()(implicit s3Client: S3Client) {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[RootFinderSummary]]

  def getSummary(
    ingestId: IngestID,
    unpackLocation: S3ObjectLocationPrefix
  ): IngestStep =
    Try {
      val startTime = Instant.now()

      s3BagLocator.locateBagRoot(unpackLocation) match {
        case Success(bagRoot) =>
          IngestStepSucceeded(
            RootFinderSuccessSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now(),
              searchRoot = unpackLocation,
              bagRoot = bagRoot
            )
          )

        case Failure(err) =>
          IngestFailed(
            RootFinderFailureSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now(),
              searchRoot = unpackLocation
            ),
            err
          )
      }
    }
}
