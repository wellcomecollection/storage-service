package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, S3}

trait IngestsFixture
    extends S3
    with LocalDynamoDb
    with WorkerServiceFixture
    with ScalaFutures {

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
          withWorkerService(queue, table, topic) { _ =>
            testWith((queue, topic, table))
          }
        }
      }
    }
  }
}
