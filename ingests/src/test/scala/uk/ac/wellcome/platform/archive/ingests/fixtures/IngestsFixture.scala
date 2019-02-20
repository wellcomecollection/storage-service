package uk.ac.wellcome.platform.archive.ingests.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.progress.models.Progress
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, S3}

trait IngestsFixture
    extends S3
    with LocalDynamoDb
    with WorkerServiceFixture
    with ScalaFutures {

  def withProgress[R](progressTracker: ProgressTracker)(
    testWith: TestWith[Progress, R]): R = {
    val createdProgress = createProgress

    whenReady(progressTracker.initialise(createdProgress)) { storedProgress =>
      testWith(storedProgress)
    }
  }

  def withConfiguredApp[R](testWith: TestWith[(Queue, Topic, Table), R]): R = {
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { topic =>
        withProgressTrackerTable { table =>
          withWorkerService(queue, table, topic) { _ =>
            testWith((queue, topic, table))
          }
        }
      }
    }
  }
}
