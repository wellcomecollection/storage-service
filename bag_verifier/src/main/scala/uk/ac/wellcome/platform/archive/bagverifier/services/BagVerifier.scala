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
  implicit
  bagReader: BagReader[_],
  resolvable: Resolvable[ObjectLocation],
  verifier: Verifier
) extends Logging {

  def verify(root: ObjectLocation): Try[IngestStepResult[VerificationSummary]] =
    Try {
      implicit val bagVerifiable = new BagVerifiable(root)
      val startTime = Instant.now()

    bagReader.get(root) match {
      case Left(e) =>
        IngestFailed(VerificationSummary.incomplete(root, e, startTime), e)
      case Right(bag) =>
        VerificationSummary.create(root, bag.verify, startTime) match {
          case success @ VerificationSuccessSummary(_, _, _, _) =>
            IngestStepSucceeded(success)
          case failure @ VerificationFailureSummary(_, _, _, _) =>
            IngestFailed(failure, InvalidBag(bag))
          case incomplete @ VerificationIncompleteSummary(_, _, _, _) =>
            IngestFailed(incomplete, incomplete.e)
        }

    }
}

case class InvalidBag(bag: Bag)
    extends Throwable(s"Invalid bag: ${bag.info.externalIdentifier}")
