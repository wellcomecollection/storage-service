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
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, _}
import uk.ac.wellcome.platform.archive.common.verify.Verification._
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagVerifier()(
  implicit bagReader: BagReader[_],
  resolvable: Resolvable[ObjectLocation],
  verifier: Verifier[_]
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

          result <- verifyChecksums(
            root = root,
            bag = bag
          )
        } yield result

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
          summary =
            VerificationIncompleteSummary(
              rootLocation = root,
              e = incomplete,
              startTime = startTime,
              endTime = Instant.now()
            ),
          e = incomplete,
          maybeUserFacingMessage = None
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
              s"${verifiedFailure.location.uri}: ${verifiedFailure.e.getMessage}"
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
          summary =
            VerificationFailureSummary(
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
