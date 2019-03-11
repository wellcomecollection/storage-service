package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import grizzled.slf4j.Logging
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.platform.archive.common.ingests.operation.{
  OperationCompleted,
  OperationFailure,
  OperationResult,
  OperationSuccess
}

import scala.concurrent.{ExecutionContext, Future}

class OperationReporter(metricsSender: MetricsSender) extends Logging {
  def report[R](requestId: UUID, result: OperationResult[R])(
    implicit ec: ExecutionContext): Future[Unit] = {
    val future = result match {
      case OperationCompleted(summary) =>
        info(s"Completed - $requestId : ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationCompleted")

      case OperationSuccess(summary) =>
        info(s"Success - $requestId: ${summary.toString}")
        metricsSender.incrementCount(metricName = "OperationSuccess")

      case OperationFailure(summary, e) =>
        error(s"Failure - $requestId : ${summary.toString}", e)
        metricsSender.incrementCount(metricName = "OperationFailure")
    }

    future.map { _ =>
      ()
    }
  }
}
