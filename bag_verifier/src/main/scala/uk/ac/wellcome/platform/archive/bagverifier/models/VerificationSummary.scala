package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagverifier.fixity._
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait VerificationSummary extends Summary {
  val rootLocation: ObjectLocationPrefix
  val fixityListResult: Option[FixityListResult]

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)

  override val fieldsToLog: Seq[(String, Any)] = {
    val baseFields = Seq(("root", rootLocation))

    fixityListResult match {
      case Some(CouldNotCreateExpectedFixityList(message)) =>
        baseFields ++ Seq(
          ("status", "incomplete"),
          ("message", message)
        )

      case Some(FixityListWithErrors(errors, correct)) =>
        baseFields ++ Seq(
          ("status", "failure"),
          ("correct", correct.size),
          ("errors", errors.size)
        )

      case Some(FixityListAllCorrect(correct)) =>
        baseFields ++ Seq(
          ("status", "success"),
          ("correct", correct.size)
        )

      case _ =>
        baseFields ++ Seq(
          ("status", "incomplete")
        )
    }
  }
}

object VerificationSummary {
  def incomplete(
    ingestId: IngestID,
    root: ObjectLocationPrefix,
    e: Throwable,
    t: Instant
  ): VerificationIncompleteSummary =
    VerificationIncompleteSummary(
      ingestId = ingestId,
      rootLocation = root,
      e = e,
      startTime = t,
      endTime = Instant.now()
    )

  def create(
    ingestId: IngestID,
    root: ObjectLocationPrefix,
    v: FixityListResult,
    t: Instant
  ): VerificationSummary = v match {
    case i @ CouldNotCreateExpectedFixityList(_) =>
      VerificationIncompleteSummary(
        ingestId = ingestId,
        rootLocation = root,
        e = i,
        startTime = t,
        endTime = Instant.now()
      )
    case f @ FixityListWithErrors(_, _) =>
      VerificationFailureSummary(
        ingestId = ingestId,
        rootLocation = root,
        fixityListResult = Some(f),
        startTime = t,
        endTime = Instant.now()
      )
    case s @ FixityListAllCorrect(_) =>
      VerificationSuccessSummary(
        ingestId = ingestId,
        rootLocation = root,
        fixityListResult = Some(s),
        startTime = t,
        endTime = Instant.now()
      )
  }
}

case class VerificationIncompleteSummary(
  ingestId: IngestID,
  rootLocation: ObjectLocationPrefix,
  e: Throwable,
  startTime: Instant,
  endTime: Instant,
  fixityListResult: Option[FixityListResult] = None
) extends VerificationSummary

case class VerificationSuccessSummary(
  ingestId: IngestID,
  rootLocation: ObjectLocationPrefix,
  fixityListResult: Some[FixityListAllCorrect],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary

case class VerificationFailureSummary(
  ingestId: IngestID,
  rootLocation: ObjectLocationPrefix,
  fixityListResult: Option[FixityListWithErrors],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary
