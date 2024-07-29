package weco.messaging.fixtures.worker

import org.apache.pekko.actor.ActorSystem
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.sqsworker.pekko.{
  PekkoSQSWorker,
  PekkoSQSWorkerConfig
}
import weco.monitoring.MetricsConfig
import weco.monitoring.memory.MemoryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait PekkoSQSWorkerFixtures extends WorkerFixtures with SQS {

  def createPekkoSQSWorkerConfig(
    queue: Queue,
    namespace: String = randomAlphanumeric()): PekkoSQSWorkerConfig =
    PekkoSQSWorkerConfig(
      metricsConfig = MetricsConfig(namespace, flushInterval = 1.second),
      sqsConfig = createSQSConfigWith(queue)
    )

  def withPekkoSQSWorker[R](
    queue: Queue,
    process: TestInnerProcess,
    namespace: String = randomAlphanumeric()
  )(testWith: TestWith[(PekkoSQSWorker[MyWork, MySummary],
                        PekkoSQSWorkerConfig,
                        MemoryMetrics,
                        CallCounter),
                       R])(implicit
                           as: ActorSystem,
                           ec: ExecutionContext): R = {
    implicit val metrics: MemoryMetrics = new MemoryMetrics()

    val config = createPekkoSQSWorkerConfig(queue, namespace)

    val callCounter = new CallCounter()
    val testProcess = (work: MyWork) =>
      createResult(process, callCounter)(ec)(work)

    val worker = new PekkoSQSWorker[MyWork, MySummary](config)(testProcess)

    testWith((worker, config, metrics, callCounter))
  }
}
