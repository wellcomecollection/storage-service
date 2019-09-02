package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.verify.{
  VerificationFailure,
  VerificationIncomplete,
  VerificationResult,
  VerificationSuccess
}
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait VerificationSummary extends Summary {
  val rootLocation: ObjectLocationPrefix
  val verification: Option[VerificationResult]

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)

  override val fieldsToLog: Seq[(String, Any)] = {
    val baseFields = Seq(("root", rootLocation))

    verification match {
      case Some(VerificationIncomplete(message)) =>
        baseFields ++ Seq(
          ("status", "incomplete"),
          ("message", message)
        )

      case Some(VerificationFailure(failed, succeeded)) =>
        baseFields ++ Seq(
          ("status", "failure"),
          ("verified", succeeded.size),
          ("failed", failed.size)
        )

      case Some(VerificationSuccess(succeeded)) =>
        baseFields ++ Seq(
          ("status", "success"),
          ("verified", succeeded.size)
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
    v: VerificationResult,
    t: Instant
  ): VerificationSummary = v match {
    case i @ VerificationIncomplete(_) =>
      VerificationIncompleteSummary(
        ingestId = ingestId,
        rootLocation = root,
        e = i,
        startTime = t,
        endTime = Instant.now()
      )
    case f @ VerificationFailure(_, _) =>
      VerificationFailureSummary(
        ingestId = ingestId,
        rootLocation = root,
        verification = Some(f),
        startTime = t,
        endTime = Instant.now()
      )
    case s @ VerificationSuccess(_) =>
      VerificationSuccessSummary(
        ingestId = ingestId,
        rootLocation = root,
        verification = Some(s),
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
  endTime: Instant
) extends VerificationSummary {
  val verification: Option[VerificationResult] = None
}

case class VerificationSuccessSummary(
  ingestId: IngestID,
  rootLocation: ObjectLocationPrefix,
  verification: Some[VerificationSuccess],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary

case class VerificationFailureSummary(
  ingestId: IngestID,
  rootLocation: ObjectLocationPrefix,
  verification: Option[VerificationFailure],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary
