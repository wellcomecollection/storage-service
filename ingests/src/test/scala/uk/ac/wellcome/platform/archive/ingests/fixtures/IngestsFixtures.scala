package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.fixtures.{SNS, SQS}
import uk.ac.wellcome.platform.archive.common.fixtures.MonitoringClientFixture
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.platform.archive.ingests.services.IngestsWorker
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, S3}

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsFixtures
    extends S3
      with SQS
      with SNS
      with Akka
      with LocalDynamoDb
      with ScalaFutures
      with IngestTrackerFixture
      with CallbackNotificationServiceFixture
      with AlpakkaSQSWorkerFixtures
      with MonitoringClientFixture {

  def withIngestWorker[R](queue: Queue, table: Table, topic: Topic)(
    testWith: TestWith[IngestsWorker, R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer { implicit materializer =>
          withIngestTracker(table) { ingestTracker =>
            withCallbackNotificationService(topic) {
              callbackNotificationService =>
                val service = new IngestsWorker(
                  alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
                  ingestTracker = ingestTracker,
                  callbackNotificationService = callbackNotificationService
                )

                service.run()

                testWith(service)
            }
          }
        }
      }
    }

  def withIngest[R](ingestTracker: IngestTracker)(
    testWith: TestWith[Ingest, R]): R = {
    val createdIngest = createIngest

    whenReady(
      ingestTracker
        .initialise(createdIngest))(testWith(_))
  }

  def withConfiguredApp[R](testWith: TestWith[(Queue, Topic, Table), R]): R = {
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { topic =>
        withIngestTrackerTable { table =>
          withIngestWorker(queue, table, topic) { _ =>
            testWith((queue, topic, table))
          }
        }
      }
    }
  }
}
