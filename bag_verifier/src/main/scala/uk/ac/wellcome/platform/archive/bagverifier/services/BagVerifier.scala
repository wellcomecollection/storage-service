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
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Try

trait BagVerifier[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]]
    extends Logging
    with VerifyChecksumAndSize[BagLocation, BagPrefix]
    with VerifyExternalIdentifier
    with VerifyFetch[BagLocation, BagPrefix]
    with VerifyPayloadOxum
    with VerifyNoUnreferencedFiles[BagLocation, BagPrefix] {

  val namespace: String

  def createPrefix(namespace: String, path: String): BagPrefix

  implicit val bagReader: BagReader[BagLocation, BagPrefix]
  implicit val resolvable: Resolvable[BagLocation]
  implicit val fixityChecker: FixityChecker[BagLocation]
  implicit val listing: Listing[BagPrefix, BagLocation]

  def verifyStandaloneBag(
    ingestId: IngestID,
    root: BagPrefix,
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
            fetch = bag.fetch,
            root = createPrefix(
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

          _ <- verificationResult match {
            case FixityListAllCorrect(_) =>
              verifyNoConcreteFetchEntries(
                fetch = bag.fetch,
                root = root,
                actualLocations = actualLocations
              )

            case _ => Right(())
          }

          _ <- verifyNoUnreferencedFiles(
            root = root,
            actualLocations = actualLocations,
            verificationResult = verificationResult
          )

          _ <- verificationResult match {
            case FixityListAllCorrect(locations) =>
              verifyPayloadOxumFileSize(bag = bag, locations = locations)

            case _ => Right(())
          }

        } yield verificationResult

      buildStepResult(
        ingestId = ingestId,
        internalResult = internalResult,
        root = root,
        startTime = startTime
      )
    }

  private def getBag(
    root: BagPrefix,
    startTime: Instant
  ): Either[BagVerifierError, Bag] =
    bagReader.get(root) match {
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
    internalResult: Either[BagVerifierError, FixityListResult[BagLocation]],
    root: BagPrefix,
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

      case Right(
          creationError: CouldNotCreateExpectedFixityList[BagLocation]
          ) =>
        IngestFailed(
          summary = VerificationIncompleteSummary(
            ingestId = ingestId,
            root = root,
            e = creationError,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = creationError,
          maybeUserFacingMessage = Some(creationError.getMessage)
        )

      case Right(success: FixityListAllCorrect[BagLocation]) =>
        IngestStepSucceeded(
          VerificationSuccessSummary(
            ingestId = ingestId,
            root = root,
            fixityListResult = Some(success),
            startTime = startTime,
            endTime = Instant.now()
          )
        )

      case Right(result: FixityListWithErrors[BagLocation]) =>
        val verificationFailureMessage =
          result.errors
            .map { fixityError: FileFixityError[BagLocation] =>
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
            root = root,
            fixityListResult = Some(result),
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = new Throwable(userFacingMessage),
          maybeUserFacingMessage = Some(userFacingMessage)
        )
    }
}

trait UnpackedBagVerifier[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] extends BagVerifier[BagLocation, BagPrefix] {
  def verifyBag(
    ingestId: IngestID,
    root: BagPrefix,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier
  ): Try[IngestStepResult[VerificationSummary]] =
    verifyStandaloneBag(
      ingestId = ingestId,
      root = root,
      space = space,
      externalIdentifier = externalIdentifier
    )
}

trait ReplicatedBagVerifier[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] extends BagVerifier[BagLocation, BagPrefix] {
  def verifyBag(
    ingestId: IngestID,
    root: BagPrefix,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier
  ): Try[IngestStepResult[VerificationSummary]] =
    verifyStandaloneBag(
      ingestId = ingestId,
      root = root,
      space = space,
      externalIdentifier = externalIdentifier
    )
}
