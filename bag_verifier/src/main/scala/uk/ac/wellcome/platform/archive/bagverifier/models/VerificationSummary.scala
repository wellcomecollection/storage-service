package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.verify.{VerificationFailure, VerificationIncomplete, VerificationResult, VerificationSuccess}
import uk.ac.wellcome.storage.ObjectLocation

sealed trait VerificationSummary extends Summary {
  val rootLocation: ObjectLocation
  val verification: Option[VerificationResult]
  val startTime: Instant
  val endTime: Option[Instant]

  override def toString: String = {

    val status = verification match {
      case Some(VerificationIncomplete(msg)) =>
        f"""
           |status=incomplete
           |message=${msg}
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

    f"""|bag=$rootLocation
        |durationSeconds=$durationSeconds
        |duration=$formatDuration
        |$status
        """.stripMargin
      .replaceAll("\n", ", ")
  }
}

object VerificationSummary {
  def incomplete(root: ObjectLocation,
                 e: Throwable,
                 t: Instant): VerificationIncompleteSummary = {
    VerificationIncompleteSummary(root, e, t)
  }

  def create(
    root: ObjectLocation,
    v: VerificationResult,
    t: Instant
  ): VerificationSummary = v match {
    case i @ VerificationIncomplete(_) =>
      VerificationIncompleteSummary(
        root,
        i,
        t,
        Some(Instant.now())
      )
    case f @ VerificationFailure(_, _) =>
      VerificationFailureSummary(
        root,
        Some(f),
        t,
        Some(Instant.now())
      )
    case s @ VerificationSuccess(_) =>
      VerificationSuccessSummary(
        root,
        Some(s),
        t,
        Some(Instant.now())
      )
  }
}

case class VerificationIncompleteSummary(rootLocation: ObjectLocation,
                                         e: Throwable,
                                         startTime: Instant,
                                         endTime: Option[Instant] = None)
    extends VerificationSummary {
  val verification = None
}

case class VerificationSuccessSummary(rootLocation: ObjectLocation,
                                      verification: Some[VerificationSuccess],
                                      startTime: Instant,
                                      endTime: Option[Instant] = None)
    extends VerificationSummary

case class VerificationFailureSummary(rootLocation: ObjectLocation,
                                      verification: Some[VerificationFailure],
                                      startTime: Instant,
                                      endTime: Option[Instant] = None)
    extends VerificationSummary
