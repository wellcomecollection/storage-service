package uk.ac.wellcome.platform.archive.common.fixtures

import akka.stream.QueueOfferResult.QueueClosed
import org.mockito.Matchers.anyString
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import uk.ac.wellcome.fixtures.{Fixture, fixture}
import uk.ac.wellcome.monitoring.MetricsSender

import scala.concurrent.Future

trait MetricsSenderFixtures extends MockitoSugar {

  def withMetricsSender[R]: Fixture[MetricsSender, R] =
    fixture[MetricsSender, R](
      create = {
        val metricsSender = mock[MetricsSender]
        when(
          metricsSender.incrementCount(anyString())
        ).thenAnswer(
          _ => {
            Future.successful(QueueClosed)
          }
        )
        metricsSender
      }
    )
}