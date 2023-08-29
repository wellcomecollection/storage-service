package weco.messaging.worker

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.messaging.fixtures.monitoring.metrics.MetricsFixtures
import weco.messaging.fixtures.worker.WorkerFixtures
import weco.monitoring.memory.MemoryMetrics

import scala.concurrent.ExecutionContext.Implicits.global

class WorkerTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with WorkerFixtures
    with MetricsFixtures {

  it("successfully processes a work and increments success metrics") {
    val namespace = s"ns-${randomAlphanumeric()}"
    implicit val metrics = new MemoryMetrics()

    val worker = new MyWorker(
      metricsNamespace = namespace,
      testProcess = successful,
      parseMessage = parseMessage(shouldFail = false)
    )

    val process = worker.process(message)
    whenReady(process) { _ =>
      worker.callCounter.calledCount shouldBe 1

      assertMetricCount(
        metrics = metrics,
        metricName = s"$namespace/Successful",
        expectedCount = 1
      )

      assertMetricDurations(
        metrics = metrics,
        metricName = s"$namespace/Duration",
        expectedNumberDurations = 1
      )
    }
  }

  it("records a terminal failure if it can't parse the message") {
    val namespace = s"ns-${randomAlphanumeric()}"
    implicit val metrics = new MemoryMetrics()

    val worker = new MyWorker(
      metricsNamespace = namespace,
      testProcess = successful,
      parseMessage = parseMessage(shouldFail = true)
    )

    val process = worker.process(message)
    whenReady(process) { _ =>
      worker.callCounter.calledCount shouldBe 0

      assertMetricCount(
        metrics = metrics,
        metricName = s"$namespace/TerminalFailure",
        expectedCount = 1
      )

      assertMetricDurations(
        metrics = metrics,
        metricName = s"$namespace/Duration",
        expectedNumberDurations = 1
      )
    }
  }

  it("doesn't increment metrics if monitoring fails") {
    val namespace = s"ns-${randomAlphanumeric()}"
    implicit val metrics = brokenMemoryMetrics

    val worker = new MyWorker(
      metricsNamespace = namespace,
      testProcess = successful,
      parseMessage = parseMessage(shouldFail = false)
    )

    val process = worker.process(message)

    whenReady(process) { _ =>
      worker.callCounter.calledCount shouldBe 1

      metrics.incrementedCounts shouldBe empty

      metrics.recordedValues shouldBe empty
    }
  }

  it("records a terminal failure if it can't process the message") {
    val namespace = s"ns-${randomAlphanumeric()}"
    implicit val metrics = new MemoryMetrics()

    val worker = new MyWorker(
      metricsNamespace = namespace,
      testProcess = terminalFailure,
      parseMessage = parseMessage(shouldFail = false)
    )

    val process = worker.process(message)
    whenReady(process) { _ =>
      worker.callCounter.calledCount shouldBe 1

      assertMetricCount(
        metrics = metrics,
        metricName = s"$namespace/TerminalFailure",
        expectedCount = 1
      )

      assertMetricDurations(
        metrics = metrics,
        metricName = s"$namespace/Duration",
        expectedNumberDurations = 1
      )
    }
  }

  it("records a retryable failure if there's a retryable error") {
    val namespace = s"ns-${randomAlphanumeric()}"
    implicit val metrics = new MemoryMetrics()

    val worker = new MyWorker(
      metricsNamespace = namespace,
      testProcess = retryableFailure,
      parseMessage = parseMessage(shouldFail = false)
    )

    val process = worker.process(message)
    whenReady(process) { _ =>
      worker.callCounter.calledCount shouldBe 1

      assertMetricCount(
        metrics = metrics,
        metricName = s"$namespace/RetryableFailure",
        expectedCount = 1
      )

      assertMetricDurations(
        metrics = metrics,
        metricName = s"$namespace/Duration",
        expectedNumberDurations = 1
      )
    }
  }
}
