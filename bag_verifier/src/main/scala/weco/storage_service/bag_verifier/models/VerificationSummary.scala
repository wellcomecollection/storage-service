package weco.storage_service.bag_verifier.models

import java.time.Instant

import org.apache.commons.io.FileUtils
import weco.storage_service.bag_verifier.fixity._
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary
import weco.storage.{Location, Prefix}

sealed trait VerificationSummary extends Summary {
  val root: Prefix[_ <: Location]
  val fixityListResult: Option[FixityListResult[_]]

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)

  override val fieldsToLog: Seq[(String, Any)] = {
    val baseFields = Seq(("root", root.toString))

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
        val totalBytes = correct.map { _.size }.sum
        baseFields ++ Seq(
          ("status", "success"),
          ("correct", correct.size),
          ("totalSize", FileUtils.byteCountToDisplaySize(totalBytes))
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
    root: Prefix[_ <: Location],
    e: Throwable,
    t: Instant
  ): VerificationIncompleteSummary =
    VerificationIncompleteSummary(
      ingestId = ingestId,
      root = root,
      e = e,
      startTime = t,
      endTime = Instant.now()
    )

  def create(
    ingestId: IngestID,
    root: Prefix[_ <: Location],
    v: FixityListResult[_],
    t: Instant
  ): VerificationSummary = v match {
    case i @ CouldNotCreateExpectedFixityList(_) =>
      VerificationIncompleteSummary(
        ingestId = ingestId,
        root = root,
        e = i,
        startTime = t,
        endTime = Instant.now()
      )
    case f @ FixityListWithErrors(_, _) =>
      VerificationFailureSummary(
        ingestId = ingestId,
        root = root,
        fixityListResult = Some(f),
        startTime = t,
        endTime = Instant.now()
      )
    case s @ FixityListAllCorrect(_) =>
      VerificationSuccessSummary(
        ingestId = ingestId,
        root = root,
        fixityListResult = Some(s),
        startTime = t,
        endTime = Instant.now()
      )
  }
}

case class VerificationIncompleteSummary(
  ingestId: IngestID,
  root: Prefix[_ <: Location],
  e: Throwable,
  startTime: Instant,
  endTime: Instant,
  fixityListResult: Option[FixityListResult[_]] = None
) extends VerificationSummary

case class VerificationSuccessSummary(
  ingestId: IngestID,
  root: Prefix[_ <: Location],
  fixityListResult: Some[FixityListAllCorrect[_]],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary

case class VerificationFailureSummary(
  ingestId: IngestID,
  root: Prefix[_ <: Location],
  fixityListResult: Option[FixityListWithErrors[_]],
  startTime: Instant,
  endTime: Instant
) extends VerificationSummary
