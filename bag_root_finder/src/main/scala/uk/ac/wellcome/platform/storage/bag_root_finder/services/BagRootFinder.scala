package uk.ac.wellcome.platform.storage.bag_root_finder.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.storage.bag_root_finder.models._
import uk.ac.wellcome.storage.ObjectLocationPrefix

import scala.util.{Failure, Success, Try}

class BagRootFinder()(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[RootFinderSummary]]

  def getSummary(unpackLocation: ObjectLocationPrefix): IngestStep =
    Try {
      val startTime = Instant.now()

      s3BagLocator.locateBagRoot(unpackLocation) match {
        case Success(rootLocation) =>
          IngestStepSucceeded(
            RootFinderSuccessSummary(
              location = unpackLocation,
              bagRootLocation = rootLocation,
              startTime = startTime,
              endTime = Some(Instant.now())
            )
          )

        case Failure(err) =>
          IngestFailed(
            RootFinderFailureSummary(
              location = unpackLocation,
              startTime = startTime,
              endTime = Some(Instant.now())
            ),
            err,
            maybeUserFacingMessage =
              // TODO: Add a toString method on ObjectLocationPrefix
              Some(
                s"Unable to find root of the bag at ${unpackLocation.namespace}/${unpackLocation.path}")
          )
      }
    }
}
