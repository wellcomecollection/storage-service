package uk.ac.wellcome.platform.storage.bagauditor.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  MonitoringClientFixture,
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.storage.bagauditor.services.{
  BagAuditor,
  BagAuditorWorker
}
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

trait BagAuditorFixtures
    extends S3
    with RandomThings
    with BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withAuditorWorker[R](
    queue: Queue = defaultQueue,
    ingestTopic: Topic,
    outgoingTopic: Topic
  )(testWith: TestWith[BagAuditorWorker, R]): R =
    withActorSystem { implicit actorSystem =>
      withIngestUpdater("auditing bag", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("auditing bag", outgoingTopic) {
          outgoingPublisher =>
            withMonitoringClient { implicit monitoringClient =>
              val worker = new BagAuditorWorker(
                alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
                bagAuditor = new BagAuditor(),
                ingestUpdater = ingestUpdater,
                outgoingPublisher = outgoingPublisher
              )

              worker.run()

              testWith(worker)
            }
        }
      }
    }
}
