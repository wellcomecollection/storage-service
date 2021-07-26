package weco.storage_service.bag_verifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import weco.storage_service.bag_verifier.fixity._
import weco.storage_service.bag_verifier.models._
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage_service.bag_verifier.verify.steps._
import weco.storage_service.bagit.models._
import weco.storage_service.bagit.services.BagReader
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models._
import weco.storage.listing.Listing
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.{store, Location, Prefix}
import EnsureTrailingSlash._
import weco.storage.streaming.InputStreamWithLength

import scala.util.Try

trait ReplicatedBagVerifier[
  ReplicaBagLocation <: Location,
  ReplicaBagPrefix <: Prefix[ReplicaBagLocation]
] extends BagVerifier[
      ReplicatedBagVerifyContext[ReplicaBagPrefix],
      ReplicaBagLocation,
      ReplicaBagPrefix
    ]
    with VerifySourceTagManifest[ReplicaBagLocation] {
  override def verifyReplicatedBag(
    context: ReplicatedBagVerifyContext[ReplicaBagPrefix],
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    bag: Bag
  ): Either[BagVerifierError, Unit] = {
    verifySourceTagManifestIsTheSame(
      srcPrefix = context.srcRoot,
      replicaPrefix = context.replicaRoot
    )
  }

  override val replicaReader
    : store.Readable[ReplicaBagLocation, InputStreamWithLength] =
    bagReader.readable
}

trait BagVerifier[BagContext <: BagVerifyContext[BagPrefix], BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] extends Logging
    with VerifyBagDeclaration[BagLocation, BagPrefix]
    with VerifyChecksumAndSize[BagLocation, BagPrefix]
    with VerifyExternalIdentifier
    with VerifyFetch[BagLocation, BagPrefix]
    with VerifyPayloadOxum
    with VerifyFilenames
    with VerifyNoUnreferencedFiles[BagLocation, BagPrefix] {

  val bagReader: BagReader[BagLocation, BagPrefix]
  val listing: Listing[BagPrefix, BagLocation]
  val resolvable: Resolvable[BagLocation]

  def verifyReplicatedBag(
    context: BagContext,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    bag: Bag
  ): Either[BagVerifierError, Unit]

  // What bucket is storing the "primary" copy of a bag?  This is the bucket
  // that should be referred to in the fetch.txt URIs.
  val primaryBucket: String

  def verify(
    ingestId: IngestID,
    bagContext: BagContext,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier
  )(implicit et: EnsureTrailingSlash[BagPrefix]) = Try {
    val startTime = Instant.now()

    val internalResult = for {
      bag <- getBag(bagContext.root, startTime = startTime)
      _ <- verifyReplicatedBag(bagContext, space, externalIdentifier, bag)
      res <- verifyBagContents(bagContext.root, space, externalIdentifier, bag)
    } yield res

    buildStepResult(
      ingestId = ingestId,
      internalResult = internalResult,
      root = bagContext.root,
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

  private def verifyBagContents(
    root: BagPrefix,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    bag: Bag
  )(
    implicit et: EnsureTrailingSlash[BagPrefix]
  ): Either[BagVerifierError, FixityListResult[BagLocation]] =
    for {
      _ <- verifyBagDeclaration(root)

      _ <- verifyExternalIdentifier(
        bag = bag,
        externalIdentifier = externalIdentifier
      )

      _ <- verifyPayloadOxumFileCount(bag)

      _ <- verifyPayloadFilenames(bag.newManifest)
      _ <- verifyTagFileFilenames(bag.newTagManifest)

      _ <- verifyFetchPrefixes(
        fetch = bag.fetch,
        root = S3ObjectLocationPrefix(
          bucket = primaryBucket,
          keyPrefix = s"$space/$externalIdentifier"
        )
      )

      filenames = (bag.newManifest.entries ++ bag.newTagManifest.entries).map {
        case (path, _) => path.value
      }
      _ <- verifyAllowedFilenames(filenames.toSeq)

      verificationResult <- verifyChecksumAndSize(
        root = root,
        bag = bag
      )

      // Bags are stored under prefixes of the form $space/$externalIdentifier/$version
      // where $version is something like v1,v2 etc.
      // When listing files for a version of a bag, say v1, we need to be sure we don't also include
      // files from other versions, like v11, v10 etc.
      // To do that we add a trailing slash if the prefix doesn't already have one
      listRoot = root.withTrailingSlash
      actualLocations <- listing.list(listRoot) match {
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
