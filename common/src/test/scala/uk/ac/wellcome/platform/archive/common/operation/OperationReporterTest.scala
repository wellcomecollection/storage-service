package uk.ac.wellcome.platform.archive.common.operation

import akka.stream.QueueOfferResult.QueueClosed
import org.mockito.Matchers.anyString
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class OperationReporterTest extends FunSpec with Matchers with RandomThings with ScalaFutures with MockitoSugar {
  it("sends a success metric") {
    withMetricsSender { metricsSender =>
      println(metricsSender)
      val operationReporter = new OperationReporter(metricsSender)

      val future = operationReporter.report(
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
      val operationReporter = new OperationReporter(metricsSender)

      val future = operationReporter.report(
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
      val operationReporter = new OperationReporter(metricsSender)

      val future = operationReporter.report(
        requestId = randomUUID,
        result = OperationCompleted(summary = "We completed the thing")
      )

      whenReady(future) { _ =>
        verify(metricsSender, times(1))
          .incrementCount(metricName = "OperationCompleted")
      }
    }
  }

  private def withMetricsSender[R]: Fixture[MetricsSender, R] =
    fixture[MetricsSender, R](
      create = {
        val metricsSender = mock[MetricsSender]
        when(
          metricsSender.incrementCount(anyString())
        ).thenAnswer(
          (invocation: InvocationOnMock) => {
            Future.successful(QueueClosed)
          }
        )
        metricsSender
      }
    )
}
