package uk.ac.wellcome.platform.archive.common.operation.services

import java.util.UUID

import grizzled.slf4j.Logging
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}

import scala.concurrent.{ExecutionContext, Future}

class DiagnosticReporter(metricsSender: MetricsSender) extends Logging {
  def report[R](requestId: UUID, result: IngestStepResult[R])(
    implicit ec: ExecutionContext): Future[Unit] = {
    val future = result match {
      case IngestCompleted(summary) =>
        info(s"Completed - $requestId - ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationCompleted")

      case IngestStepSucceeded(summary) =>
        info(s"Success - $requestId - ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationSuccess")

      case IngestFailed(summary, e, _) =>
        error(
          s"Failure - $requestId - ${e.getClass.getSimpleName} '${e.getMessage}' - ${summary.toString}",
          e)
        metricsSender.incrementCount(metricName = "OperationFailure")
    }
    future.map { _ =>
      ()
    }
  }
}
