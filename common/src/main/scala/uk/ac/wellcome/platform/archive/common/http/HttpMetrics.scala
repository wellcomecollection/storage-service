package uk.ac.wellcome.platform.archive.common.http

import akka.http.scaladsl.model.{HttpResponse, StatusCode}
import grizzled.slf4j.Logging
import uk.ac.wellcome.monitoring.Metrics

import scala.concurrent.Future

object HttpMetricResults extends Enumeration {
  type HttpMetricResults = Value
  val Success, UserError, ServerError, Unrecognised = Value
}

class HttpMetrics(name: String, metrics: Metrics[Future, _]) extends Logging {

  def sendMetric(resp: HttpResponse): Future[Unit] =
    sendMetricForStatus(resp.status)

  def sendMetricForStatus(status: StatusCode): Future[Unit] = {
    val httpMetric = if (status.isSuccess()) {
      HttpMetricResults.Success
    } else if (status.isFailure() && status.intValue() < 500) {
      HttpMetricResults.UserError
    } else if (status.isFailure()) {
      HttpMetricResults.ServerError
    } else {
      warn(s"Sending unexpected response code: $status")
      HttpMetricResults.Unrecognised
    }

    metrics.incrementCount(
      metricName = s"${name}_HttpResponse_$httpMetric")
  }
}
