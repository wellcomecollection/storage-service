package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity._
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.verify.steps._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.{
  Location,
  ObjectLocation,
  ObjectLocationPrefix,
  Prefix
}

import scala.util.Try

class BagVerifier[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]](
  namespace: String,
  // TODO: Temporary while we disambiguate ObjectLocation.  Remove eventually.
  toLocation: ObjectLocationPrefix => BagPrefix
)(
  implicit bagReader: BagReader[BagLocation, BagPrefix],
  val resolvable: Resolvable[ObjectLocation],
  val fixityChecker: FixityChecker[_],
  listing: Listing[ObjectLocationPrefix, ObjectLocation]
) extends Logging
    with VerifyChecksumAndSize
    with VerifyExternalIdentifier
    with VerifyFetch
    with VerifyPayloadOxum
    with VerifyNoUnreferencedFiles {

  def verify(
    ingestId: IngestID,
    root: ObjectLocationPrefix,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier
  ): Try[IngestStepResult[VerificationSummary]] =
    Try {
      val startTime = Instant.now()

      val internalResult =
        for {
          bag <- getBag(root, startTime = startTime)

          _ <- verifyExternalIdentifier(
            bag = bag,
            externalIdentifier = externalIdentifier
          )

          _ <- verifyPayloadOxumFileCount(bag)

          _ <- verifyFetchPrefixes(
            bag,
            root = ObjectLocationPrefix(
              namespace = namespace,
              path = s"$space/$externalIdentifier"
            )
          )

          verificationResult <- verifyChecksumAndSize(
            root = root,
            bag = bag
          )

          actualLocations <- listing.list(root) match {
            case Right(iterable) => Right(iterable.toSeq)
            case Left(listingFailure) =>
              Left(BagVerifierError(listingFailure.e))
          }

          _ <- verifyNoConcreteFetchEntries(
            bag = bag,
            root = root,
            actualLocations = actualLocations,
            verificationResult = verificationResult
          )

          _ <- verifyNoUnreferencedFiles(
            root = root,
            actualLocations = actualLocations,
            verificationResult = verificationResult
          )

          _ <- verifyPayloadOxumFileSize(
            bag = bag,
            verificationResult = verificationResult
          )

        } yield verificationResult

      buildStepResult(
        ingestId = ingestId,
        internalResult = internalResult,
        root = root,
        startTime = startTime
      )
    }

  private def getBag(
    root: ObjectLocationPrefix,
    startTime: Instant
  ): Either[BagVerifierError, Bag] =
    bagReader.get(toLocation(root)) match {
      case Left(bagUnavailable) =>
        Left(
          BagVerifierError(
            e = bagUnavailable,
            userMessage = Some(bagUnavailable.msg)
          )
        )

      case Right(bag) => Right(bag)
    }

  private def buildStepResult(
    ingestId: IngestID,
    internalResult: Either[BagVerifierError, FixityListResult],
    root: ObjectLocationPrefix,
    startTime: Instant
  ): IngestStepResult[VerificationSummary] =
    internalResult match {
      case Left(error) =>
        IngestFailed(
          summary = VerificationSummary.incomplete(
            ingestId = ingestId,
            root = root,
            e = error.e,
            t = startTime
          ),
          e = error.e,
          maybeUserFacingMessage = error.userMessage
        )

      case Right(creationError: CouldNotCreateExpectedFixityList) =>
        IngestFailed(
          summary = VerificationIncompleteSummary(
            ingestId = ingestId,
            rootLocation = root,
            e = creationError,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = creationError,
          maybeUserFacingMessage = Some(creationError.getMessage)
        )

      case Right(success: FixityListAllCorrect) =>
        IngestStepSucceeded(
          VerificationSuccessSummary(
            ingestId = ingestId,
            rootLocation = root,
            fixityListResult = Some(success),
            startTime = startTime,
            endTime = Instant.now()
          )
        )

      case Right(result: FixityListWithErrors) =>
        val verificationFailureMessage =
          result.errors
            .map { fixityError: FileFixityError =>
              s"${fixityError.expectedFileFixity.uri}: ${fixityError.e.getMessage}"
            }
            .mkString("\n")

        warn(s"Errors verifying $root:\n$verificationFailureMessage")

        val errorCount = result.errors.size
        val pathList =
          result.errors.map { _.expectedFileFixity.path.value }.mkString(", ")

        val userFacingMessage =
          if (errorCount == 1)
            s"Unable to verify one file in the bag: $pathList"
          else
            s"Unable to verify $errorCount files in the bag: $pathList"

        IngestFailed(
          summary = VerificationFailureSummary(
            ingestId = ingestId,
            rootLocation = root,
            fixityListResult = Some(result),
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = new Throwable(userFacingMessage),
          maybeUserFacingMessage = Some(userFacingMessage)
        )
    }
}
