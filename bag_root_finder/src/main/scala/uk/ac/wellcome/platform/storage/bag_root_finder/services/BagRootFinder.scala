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
import uk.ac.wellcome.storage.{ObjectLocationPrefix, S3ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class BagRootFinder()(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[RootFinderSummary]]

  def getSummary(
    ingestId: IngestID,
    unpackLocation: ObjectLocationPrefix
  ): IngestStep =
    Try {
      val startTime = Instant.now()

      val s3SearchRoot = S3ObjectLocationPrefix(unpackLocation)

      s3BagLocator.locateBagRoot(s3SearchRoot) match {
        case Success(bagRoot) =>
          IngestStepSucceeded(
            RootFinderSuccessSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now(),
              searchRoot = s3SearchRoot,
              bagRoot = bagRoot
            )
          )

        case Failure(err) =>
          IngestFailed(
            RootFinderFailureSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now(),
              searchRoot = s3SearchRoot
            ),
            err
          )
      }
    }
}
