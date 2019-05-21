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

import scala.util.Try

class BagVerifier()(
  implicit
  bagService: BagService,
  verifier: Verifier
) extends Logging {

  private def summarise(root: ObjectLocation, bag: Bag, startTime: Instant): IngestStepResult[VerificationSummary] = {
    VerificationSummary.create(root, bag.verify, startTime) match {
      case success@VerificationSuccessSummary(_, _, _, _) =>
        IngestStepSucceeded(success)
      case failure@VerificationFailureSummary(_, _, _, _) =>
        IngestFailed(failure, new RuntimeException("Invalid bag!"))
      case incomplete@VerificationIncompleteSummary(_, _, _, _) =>
        IngestFailed(incomplete, new RuntimeException("Could not verify!"))
    }
  }

  def verify(root: ObjectLocation) = Try {
    val startTime = Instant.now()

    val verification = bagService.retrieve(root) match {
      case Left(e) => IngestFailed(VerificationSummary.incomplete(root, e, startTime), e)
      case Right(bag) => summarise(root, bag, startTime)
    }

    verification
  }
}