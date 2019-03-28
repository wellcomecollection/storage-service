package uk.ac.wellcome.platform.archive.common.operation.services

import java.util.UUID

import grizzled.slf4j.Logging
import uk.ac.wellcome.monitoring.MetricsSender

import scala.concurrent.{ExecutionContext, Future}

class DiagnosticReporter(metricsSender: MetricsSender) extends Logging {
  def report[R](requestId: UUID, result: IngestStepResult[R])(
    implicit ec: ExecutionContext): Future[Unit] = {
    val future = result match {
      case IngestCompleted(summary) =>
        info(s"Completed - $requestId - ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationCompleted")

      case IngestStepSuccess(summary) =>
        info(s"Success - $requestId - ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationSuccess")

      case IngestFailed(summary, e) =>
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
