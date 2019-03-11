package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import grizzled.slf4j.Logging

object OperationReporter extends Logging {
  def report[R](requestId: UUID, result: OperationResult[R]): Unit =
    result match {
      case OperationCompleted(summary) =>
        info(s"Completed - $requestId : ${summary.toString}")

      case OperationSuccess(summary) =>
        info(s"Success - $requestId: ${summary.toString}")

      case OperationFailure(summary, e) =>
        error(s"Failure - $requestId : ${summary.toString}", e)
    }
}
