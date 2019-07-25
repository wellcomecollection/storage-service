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
import uk.ac.wellcome.platform.archive.common.verify.Verifier
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

class BagVerifier()(
  implicit bagReader: BagReader[_],
  resolvable: Resolvable[ObjectLocation],
  verifier: Verifier[_]
) extends Logging {

  type InternalResult[T] = Either[IngestFailed[VerificationSummary], T]

  def verify(root: ObjectLocation, externalIdentifier: ExternalIdentifier)
    : Try[IngestStepResult[VerificationSummary]] =
    Try {
      val startTime = Instant.now()

      val stepResult
        : Either[IngestFailed[VerificationSummary], VerificationSuccessSummary] = for {
        bag <- getBag(root, startTime = startTime)

        _ <- verifyExternalIdentifier(
          bag = bag,
          externalIdentifier = externalIdentifier,
          root = root,
          startTime = startTime
        )

        result <- verifyChecksums(
          root = root,
          bag = bag,
          startTime = startTime
        )
      } yield result

      stepResult match {
        case Left(ingestFailed)    => ingestFailed
        case Right(successSummary) => IngestStepSucceeded(successSummary: VerificationSuccessSummary)
      }
    }

  private def getBag(root: ObjectLocation,
                     startTime: Instant): InternalResult[Bag] =
    bagReader.get(root) match {
      case Left(bagUnavailable) =>
        Left(
          IngestFailed(
            summary =
              VerificationSummary.incomplete(root, bagUnavailable, startTime),
            e = bagUnavailable,
            maybeUserFacingMessage = Some(bagUnavailable.msg)
          )
        )

      case Right(bag) => Right(bag)
    }

  private def verifyExternalIdentifier(
    bag: Bag,
    externalIdentifier: ExternalIdentifier,
    root: ObjectLocation,
    startTime: Instant): InternalResult[Unit] =
    if (bag.info.externalIdentifier != externalIdentifier) {
      Left(
        IngestFailed(
          summary = VerificationFailureSummary(
            rootLocation = root,
            verification = None,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = new Throwable(
            "External identifier in bag-info.txt does not match request"),
          maybeUserFacingMessage = Some(
            s"External identifier in bag-info.txt does not match request: ${bag.info.externalIdentifier.underlying} is not ${externalIdentifier.underlying}"
          )
        )
      )
    } else {
      Right(())
    }

  private def verifyChecksums(
    root: ObjectLocation,
    bag: Bag,
    startTime: Instant
  ): InternalResult[VerificationSuccessSummary] = {
    implicit val bagVerifiable: BagVerifiable =
      new BagVerifiable(root)

    VerificationSummary.create(root, bag.verify, startTime) match {
      case success @ VerificationSuccessSummary(_, _, _, _) =>
        Right(success)
      case failure @ VerificationFailureSummary(_, Some(verification), _, _) =>
        val verificationFailureMessage =
          verification.failure
            .map { verifiedFailure =>
              s"${verifiedFailure.location.uri}: ${verifiedFailure.e.getMessage}"
            }
            .mkString("\n")

        warn(s"Errors verifying $root:\n$verificationFailureMessage")

        val errorCount = verification.failure.size

        val userFacingMessage =
          if (errorCount == 1)
            "There was 1 error verifying the bag"
          else
            s"There were $errorCount errors verifying the bag"

        Left(IngestFailed(failure, InvalidBag(bag), Some(userFacingMessage)))

      case failure @ VerificationFailureSummary(_, None, _, _) =>
        Left(IngestFailed(failure, InvalidBag(bag)))

      case incomplete @ VerificationIncompleteSummary(_, _, _, _) =>
        Left(IngestFailed(incomplete, incomplete.e))
    }
  }
}

case class InvalidBag(bag: Bag)
    extends Throwable(s"Invalid bag: ${bag.info.externalIdentifier}")
