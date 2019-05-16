package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.BagService
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.verify.Verification._
import uk.ac.wellcome.platform.archive.common.verify.Verifier
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagVerifier()(
  implicit
    bagService: BagService,
    verifier: Verifier
) extends Logging {

  type IngestStep = Try[IngestStepResult[VerificationSummary]]

  def verify(root: ObjectLocation): IngestStep = Try {
    val startTime = Instant.now()

    val verification = bagService.retrieve(root).map { bag =>
      implicit val verifiable = bag.verifiable(root)

      VerificationSummary.create(root, bag.verify, startTime)
    } recover {
      case e => VerificationSummary.incomplete(root, e, startTime)
    }

    verification match {
      case Success(success@VerificationSuccessSummary(_,_,_,_)) =>
        IngestStepSucceeded(success)
      case Success(failure@VerificationFailureSummary(_,_,_,_)) =>
        IngestFailed(
          failure,
          new RuntimeException("Invalid bag!"))
      case Success(failure@VerificationIncompleteSummary(_,e,_,_)) =>
        IngestFailed(
          failure,
          new RuntimeException("Could not verify!"))
      case Failure(e) =>
        IngestFailed(
          VerificationSummary.incomplete(root, new UnknownError(), startTime),
          e)
    }
  }
}
