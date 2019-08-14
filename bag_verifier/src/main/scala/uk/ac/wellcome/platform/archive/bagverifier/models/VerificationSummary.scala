package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.verify.{
  VerificationFailure,
  VerificationIncomplete,
  VerificationResult,
  VerificationSuccess
}
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait VerificationSummary extends Summary {
  val bagRoot: ObjectLocationPrefix
  val verification: Option[VerificationResult]

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)

  override def toString: String = {

    val status = verification match {
      case Some(VerificationIncomplete(msg)) =>
        f"""
           |status=incomplete
           |message=$msg
         """.stripMargin
      case Some(VerificationFailure(failed, succeeded)) =>
        f"""
           |status=failure
           |verified=${succeeded.size}
           |failed=${failed.size}
         """.stripMargin
      case Some(VerificationSuccess(succeeded)) =>
        f"""
           |status=success
           |verified=${succeeded.size}
         """.stripMargin
      case None =>
        f"""
           |status=incomplete
        """.stripMargin

    }

    f"""|bag=$bagRoot
        |durationSeconds=$durationSeconds
        |duration=$formatDuration
        |$status
        """.stripMargin
      .replaceAll("\n", ", ")
  }
}

object VerificationSummary {
  def incomplete(
    bagRoot: ObjectLocationPrefix,
    e: Throwable,
    t: Instant
  ): VerificationIncompleteSummary =
    VerificationIncompleteSummary(
      bagRoot = bagRoot,
      e = e,
      startTime = t,
      endTime = Instant.now()
    )

  def create(
    root: ObjectLocationPrefix,
    v: VerificationResult,
    t: Instant
  ): VerificationSummary = v match {
    case i @ VerificationIncomplete(_) =>
      VerificationIncompleteSummary(
        bagRoot = root,
        e = i,
        startTime = t,
        endTime = Instant.now()
      )
    case f @ VerificationFailure(_, _) =>
      VerificationFailureSummary(
        bagRoot = root,
        verification = Some(f),
        startTime = t,
        endTime = Instant.now()
      )
    case s @ VerificationSuccess(_) =>
      VerificationSuccessSummary(
        bagRoot = root,
        verification = Some(s),
        startTime = t,
        endTime = Instant.now()
      )
  }
}

case class VerificationIncompleteSummary(
  bagRoot: ObjectLocationPrefix,
  e: Throwable,
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary {
  val verification: None.type = None
}

case class VerificationSuccessSummary(
  bagRoot: ObjectLocationPrefix,
  verification: Some[VerificationSuccess],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary

case class VerificationFailureSummary(
  bagRoot: ObjectLocationPrefix,
  verification: Option[VerificationFailure],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary
