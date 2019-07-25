package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.{BagReader, BagVerifiable}
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

  def verify(
              root: ObjectLocation,
              externalIdentifier: ExternalIdentifier
  ): Try[IngestStepResult[VerificationSummary]] = Try {
      implicit val bagVerifiable: BagVerifiable =
        new BagVerifiable(root)
      val startTime = Instant.now()

      bagReader.get(root) match {
        case Left(e) =>
          IngestFailed(
            summary = VerificationSummary
              .incomplete(root, e, startTime),
            e = e
          )

        case Right(bag) =>
          if (bag.info.externalIdentifier != externalIdentifier) IngestFailed(
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
          ) else VerificationSummary.create(
            root = root,
            v = bag.verify,
            t = startTime
          ) match {
            case success @ VerificationSuccessSummary(_, _, _, _) =>
              IngestStepSucceeded(success)
            case failure @ VerificationFailureSummary(_, _, _, _) =>
              IngestFailed(
                summary = failure,
                e = InvalidBag(bag)
              )
            case incomplete @ VerificationIncompleteSummary(_, _, _, _) =>
              IngestFailed(
                summary = incomplete,
                e = incomplete.e
              )
          }
      }
    }
}

case class InvalidBag(bag: Bag) extends Throwable(
  s"Invalid bag: ${bag.info.externalIdentifier}"
)
