package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagVerifiable
}
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.verify.Verification._
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class BagVerifier()(
  implicit bagReader: BagReader[_],
  resolvable: Resolvable[ObjectLocation],
  verifier: Verifier[_],
  listing: Listing[ObjectLocationPrefix, ObjectLocation],
) extends Logging {

  case class BagVerifierError(
    e: Throwable,
    userMessage: Option[String] = None
  )

  type InternalResult[T] = Either[BagVerifierError, T]

  def verify(root: ObjectLocation, externalIdentifier: ExternalIdentifier)
    : Try[IngestStepResult[VerificationSummary]] =
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

          verificationResult <- verifyChecksums(
            root = root,
            bag = bag
          )

          actualLocations <- listing.list(root.asPrefix) match {
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

      buildStepResult(internalResult, root = root, startTime = startTime)
    }

  private def getBag(root: ObjectLocation,
                     startTime: Instant): InternalResult[Bag] =
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

  private def verifyExternalIdentifier(
    bag: Bag,
    externalIdentifier: ExternalIdentifier): InternalResult[Unit] =
    if (bag.info.externalIdentifier != externalIdentifier) {
      val message =
        "External identifier in bag-info.txt does not match request: " +
          s"${bag.info.externalIdentifier.underlying} is not ${externalIdentifier.underlying}"

      Left(
        BagVerifierError(
          e = new Throwable(message),
          userMessage = Some(message)
        )
      )
    } else {
      Right(())
    }

  private def verifyChecksums(
    root: ObjectLocation,
    bag: Bag
  ): InternalResult[VerificationResult] = {
    implicit val bagVerifiable: BagVerifiable =
      new BagVerifiable(root)

    Try { bag.verify } match {
      case Failure(err)    => Left(BagVerifierError(err))
      case Success(result) => Right(result)
    }
  }

  private def verifyPayloadOxumFileCount(bag: Bag): InternalResult[Unit] = {
    val payloadOxumCount = bag.info.payloadOxum.numberOfPayloadFiles
    val manifestCount = bag.manifest.files.size

    if (payloadOxumCount != bag.manifest.files.size) {
      val message =
        s"Payload-Oxum has the wrong number of payload files: $payloadOxumCount, but bag manifest has $manifestCount"
      Left(
        BagVerifierError(new Throwable(message), userMessage = Some(message))
      )
    } else {
      Right(())
    }
  }

  private def verifyPayloadOxumFileSize(
    bag: Bag,
    verificationResult: VerificationResult): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(locations) =>
        // The Payload-Oxum octetstream sum only counts the size of files in the payload,
        // not manifest files such as the bag-info.txt file.
        // We need to filter those out.
        val dataFilePaths = bag.manifest.files.map { _.path }

        val actualSize =
          locations
            .filter { loc =>
              dataFilePaths.contains(loc.verifiableLocation.path)
            }
            .map { _.size }
            .sum

        val expectedSize = bag.info.payloadOxum.payloadBytes

        if (actualSize == expectedSize) {
          Right(())
        } else {
          val message =
            s"Payload-Oxum has the wrong octetstream sum: $expectedSize bytes, but bag actually contains $actualSize bytes"
          Left(
            BagVerifierError(
              new Throwable(message),
              userMessage = Some(message))
          )
        }

      case _ => Right(())
    }

  // Check that the user hasn't sent any files in the bag which
  // also have a fetch file entry.
  private def verifyNoConcreteFetchEntries(
    bag: Bag,
    root: ObjectLocation,
    actualLocations: Seq[ObjectLocation],
    verificationResult: VerificationResult): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(_) =>
        val bagFetchLocations = bag.fetch match {
          case Some(fetchEntry) =>
            fetchEntry.files
              .map { _.path }
              .map { path => root.join(path.value) }

          case None => Seq.empty
        }

        val concreteFetchLocations =
          bagFetchLocations
            .filter { actualLocations.contains(_) }

        if (concreteFetchLocations.isEmpty) {
          Right(())
        } else {
          val messagePrefix =
            "Files referred to in the fetch.txt also appear in the bag: "

          val internalMessage = messagePrefix + concreteFetchLocations.mkString(
            ", ")

          val userMessage = messagePrefix +
            concreteFetchLocations
              .map { _.path.stripPrefix(root.path).stripPrefix("/") }
              .mkString(", ")

          Left(
            BagVerifierError(
              new Throwable(internalMessage),
              userMessage = Some(userMessage)
            )
          )
        }

      case _ => Right(())
    }

  // Check that there aren't any files in the bag that aren't referenced in
  // either the file manifest or the tag manifest.
  private def verifyNoUnreferencedFiles(
    root: ObjectLocation,
    actualLocations: Seq[ObjectLocation],
    verificationResult: VerificationResult): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(locations) =>
        val expectedLocations = locations.map { _.objectLocation }

        debug(s"Expecting the bag to contain: $expectedLocations")

        val unreferencedFiles = actualLocations
          .filterNot { expectedLocations.contains(_) }
          .filterNot {
            // The tag manifest isn't referred to by other files, so we don't have
            // it in the list of verifier successes/failures.  But we expect to
            // see it in the bag.
            _ == root.join("tagmanifest-sha256.txt")
          }

        if (unreferencedFiles.isEmpty) {
          Right(())
        } else {
          // For internal logging, we want a message that contains the full
          // S3 locations for easy debugging, e.g.:
          //
          //    Bag contains 5 files which are not referenced in the manifest:
          //    bukkit/ingest-id/bag-id/unreferenced1.txt, ...
          //
          // For the user-facing message, we want to trim the first part,
          // because it's an internal detail of the storage service, e.g.:
          //
          //    Bag contains 5 files which are not referenced in the manifest:
          //    unreferenced1.txt, ...
          //
          val messagePrefix =
          if (unreferencedFiles.size == 1) {
            "Bag contains a file which is not referenced in the manifest: "
          } else {
            s"Bag contains ${unreferencedFiles.size} files which are not referenced in the manifest: "
          }

          val userMessage = messagePrefix +
            unreferencedFiles
              .map { _.path.stripPrefix(root.path) }
              .mkString(", ")

          Left(
            BagVerifierError(
              new Throwable(messagePrefix + unreferencedFiles.mkString(", ")),
              userMessage = Some(userMessage)
            ))
        }

      case _ => Right(())
    }

  private def buildStepResult(
    internalResult: InternalResult[VerificationResult],
    root: ObjectLocation,
    startTime: Instant): IngestStepResult[VerificationSummary] =
    internalResult match {
      case Left(error) =>
        IngestFailed(
          summary = VerificationSummary.incomplete(
            root = root,
            e = error.e,
            t = startTime
          ),
          e = error.e,
          maybeUserFacingMessage = error.userMessage
        )

      case Right(incomplete: VerificationIncomplete) =>
        IngestFailed(
          summary = VerificationIncompleteSummary(
            rootLocation = root,
            e = incomplete,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = incomplete,
          maybeUserFacingMessage = Some(incomplete.getMessage)
        )

      case Right(success: VerificationSuccess) =>
        IngestStepSucceeded(
          VerificationSuccessSummary(
            rootLocation = root,
            verification = Some(success),
            startTime = startTime,
            endTime = Instant.now()
          )
        )

      case Right(result: VerificationFailure) =>
        val verificationFailureMessage =
          result.failure
            .map { verifiedFailure =>
              s"${verifiedFailure.verifiableLocation.uri}: ${verifiedFailure.e.getMessage}"
            }
            .mkString("\n")

        warn(s"Errors verifying $root:\n$verificationFailureMessage")

        val errorCount = result.failure.size

        val userFacingMessage =
          if (errorCount == 1)
            "There was 1 error verifying the bag"
          else
            s"There were $errorCount errors verifying the bag"

        IngestFailed(
          summary = VerificationFailureSummary(
            rootLocation = root,
            verification = Some(result),
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = new Throwable(userFacingMessage),
          maybeUserFacingMessage = Some(userFacingMessage)
        )
    }
}
