package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagVerifiable
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.verify.Verification._
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class BagVerifier(namespace: String)(
  implicit bagReader: BagReader[_],
  resolvable: Resolvable[ObjectLocation],
  verifier: Verifier[_],
  listing: Listing[ObjectLocationPrefix, ObjectLocation]
) extends Logging {

  case class BagVerifierError(
    e: Throwable,
    userMessage: Option[String] = None
  )

  type InternalResult[T] = Either[BagVerifierError, T]

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

          verificationResult <- verifyChecksums(
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
  ): InternalResult[Bag] =
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
    externalIdentifier: ExternalIdentifier
  ): InternalResult[Unit] =
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
    root: ObjectLocationPrefix,
    bag: Bag
  ): InternalResult[VerificationResult] = {
    implicit val bagVerifiable: BagVerifiable =
      new BagVerifiable(root.asLocation())

    Try { bag.verify } match {
      case Failure(err: Throwable) => Left(BagVerifierError(err))
      case Success(result)         => Right(result)
    }
  }

  private def verifyPayloadOxumFileCount(bag: Bag): InternalResult[Unit] = {
    val payloadOxumCount = bag.info.payloadOxum.numberOfPayloadFiles
    val manifestCount = bag.manifest.entries.size

    if (payloadOxumCount != bag.manifest.entries.size) {
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
    verificationResult: VerificationResult
  ): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(locations) =>
        // The Payload-Oxum octetstream sum only counts the size of files in the payload,
        // not manifest files such as the bag-info.txt file.
        // We need to filter those out.
        val dataFilePaths = bag.manifest.paths

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
              userMessage = Some(message)
            )
          )
        }

      case _ => Right(())
    }

  // Check the user hasn't supplied any fetch entries whcih are in the wrong
  // namespace or path prefix.
  //
  // We only allow fetch entries to refer to previous versions of the same bag; this
  // ensures that a single bag (same space/external identifier) is completely self-contained,
  // and we don't have to worry about interconnected bag dependencies.
  private def verifyFetchPrefixes(
    bag: Bag,
    root: ObjectLocationPrefix
  ): InternalResult[Unit] =
    bag.fetch match {
      case None => Right(())

      case Some(BagFetch(entries)) =>
        val mismatchedEntries =
          entries
            .filterNot {
              case (_, fetchMetadata: BagFetchMetadata) =>
                val fetchLocation = ObjectLocation(
                  namespace = fetchMetadata.uri.getHost,
                  path = fetchMetadata.uri.getPath.stripPrefix("/")
                )

                // TODO: This could verify the version prefix as well.
                // TODO: Hard-coding the expected scheme here isn't ideal
                fetchMetadata.uri.getScheme == "s3" &&
                fetchLocation.namespace == root.namespace &&
                fetchLocation.path.startsWith(s"${root.path}/")
            }

        val mismatchedPaths = mismatchedEntries.keys.toSeq

        mismatchedPaths match {
          case Nil => Right(())
          case _ =>
            val message =
              s"fetch.txt refers to paths in a mismatched prefix: ${mismatchedPaths.mkString(", ")}"

            Left(
              BagVerifierError(
                e = new Throwable(message),
                userMessage = Some(message)
              )
            )
        }
    }

  // Check that the user hasn't sent any files in the bag which
  // also have a fetch file entry.
  private def verifyNoConcreteFetchEntries(
    bag: Bag,
    root: ObjectLocationPrefix,
    actualLocations: Seq[ObjectLocation],
    verificationResult: VerificationResult
  ): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(_) =>
        val bagFetchLocations = bag.fetch match {
          case Some(bagFetch) =>
            bagFetch.paths
              .map { path: BagPath =>
                root.asLocation(path.value)
              }

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
            ", "
          )

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

  // Files that it's okay not to be referenced by any other manifests/files.
  //
  // We ignore the tag manifests because they're not referred to by the
  // checksum lists in any other manifests:
  //
  //        (tag manifest) -> (file manifest) -> (files)
  //
  // We don't ignore the file manifests (e.g. manifest-md5.txt), because
  // those should be included in the SHA256 tag manifest.  Every tag manifest
  // should include checksums for every file manifest.
  //
  private val excludedFiles = UnreferencedFiles.tagManifestFiles

  // Check that there aren't any files in the bag that aren't referenced in
  // either the file manifest or the tag manifest.
  private def verifyNoUnreferencedFiles(
    root: ObjectLocationPrefix,
    actualLocations: Seq[ObjectLocation],
    verificationResult: VerificationResult
  ): InternalResult[Unit] =
    verificationResult match {
      case VerificationSuccess(locations) =>
        val expectedLocations = locations.map { _.objectLocation }

        debug(s"Expecting the bag to contain: $expectedLocations")

        val unreferencedFiles = actualLocations
          .filterNot { expectedLocations.contains(_) }
          .filterNot { location =>
            excludedFiles.exists { root.asLocation(_) == location }
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
            )
          )
        }

      case _ => Right(())
    }

  private def buildStepResult(
    ingestId: IngestID,
    internalResult: InternalResult[VerificationResult],
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

      case Right(incomplete: VerificationIncomplete) =>
        IngestFailed(
          summary = VerificationIncompleteSummary(
            ingestId = ingestId,
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
            ingestId = ingestId,
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
        val pathList =
          result.failure.map { _.verifiableLocation.path.value }.mkString(", ")

        val userFacingMessage =
          if (errorCount == 1)
            s"Unable to verify one file in the bag: $pathList"
          else
            s"Unable to verify $errorCount files in the bag: $pathList"

        IngestFailed(
          summary = VerificationFailureSummary(
            ingestId = ingestId,
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
