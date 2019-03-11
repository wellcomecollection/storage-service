package uk.ac.wellcome.platform.archive.common.operation

import org.mockito.Mockito.{times, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MetricsSenderFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.ingests.operation.{
  OperationCompleted,
  OperationFailure,
  OperationSuccess
}

import scala.concurrent.ExecutionContext.Implicits.global

class OperationReporterTest
    extends FunSpec
    with Matchers
    with RandomThings
    with ScalaFutures
    with MetricsSenderFixtures {
  it("sends a success metric") {
    withMetricsSender { metricsSender =>
      println(metricsSender)
      val reporter = new OperationReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = OperationSuccess(summary = "A good thing happened")
      )

      whenReady(future) { result =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationSuccess")
      }
    }
  }

  it("sends a failure metric") {
    withMetricsSender { metricsSender =>
      val reporter = new OperationReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = OperationFailure(
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
    withMetricsSender { metricsSender =>
      val reporter = new OperationReporter(metricsSender)

      val future = reporter.report(
        requestId = randomUUID,
        result = OperationCompleted(summary = "We completed the thing")
      )

      whenReady(future) { _ =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationCompleted")
      }
    }
  }
}
