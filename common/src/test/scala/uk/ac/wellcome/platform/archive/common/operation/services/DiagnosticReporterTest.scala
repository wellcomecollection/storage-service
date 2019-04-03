package uk.ac.wellcome.platform.archive.common.operation.services

import org.mockito.Mockito.{times, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.concurrent.ExecutionContext.Implicits.global

class DiagnosticReporterTest
    extends FunSpec
    with Matchers
    with RandomThings
    with ScalaFutures
    with MetricsSenderFixture {
  it("sends a success metric") {
    withMockMetricsSender { metricsSender =>
      val reporter = new DiagnosticReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = IngestStepSuccess(summary = "A good thing happened")
      )

      whenReady(future) { result =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationSuccess")
      }
    }
  }

  it("sends a failure metric") {
    withMockMetricsSender { metricsSender =>
      val reporter = new DiagnosticReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = IngestFailed(
          summary = "A sad thing occurred",
          e = new RuntimeException(":(")
        )
      )

      whenReady(future) { _ =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationFailure")
      }
    }
  }

  it("sends a completed metric") {
    withMockMetricsSender { metricsSender =>
      val reporter = new DiagnosticReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = IngestCompleted(summary = "We completed the thing")
      )

      whenReady(future) { _ =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationCompleted")
      }
    }
  }
}
