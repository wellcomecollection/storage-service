package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.worker.MetricsFixtures

import scala.concurrent.ExecutionContext.Implicits.global

trait MonitoringClientFixture extends MetricsFixtures {
  def withMonitoringClient[R](
    testWith: TestWith[FakeMonitoringClient, R]): R =
    testWith(new FakeMonitoringClient())
}
