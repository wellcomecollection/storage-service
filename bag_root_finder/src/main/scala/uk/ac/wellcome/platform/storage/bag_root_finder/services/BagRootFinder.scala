package uk.ac.wellcome.platform.storage.bag_root_finder.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.storage.bag_root_finder.models._
import uk.ac.wellcome.storage.S3ObjectLocationPrefix

import scala.util.{Failure, Success, Try}

class BagRootFinder()(implicit s3Client: AmazonS3) {
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
