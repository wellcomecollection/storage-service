package uk.ac.wellcome.platform.archive.progress_async.fixtures

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.fixtures.{
  ArchiveMessaging,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.progress.fixtures.ProgressTrackerFixture
import uk.ac.wellcome.platform.archive.common.progress.models.{
  Progress,
  ProgressUpdate
}
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker
import uk.ac.wellcome.platform.archive.progress_async.flows.ProgressUpdateFlow
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, S3}

trait IngestsFixture
    extends S3
    with Akka
    with LocalDynamoDb
    with RandomThings
    with ArchiveMessaging
    with ProgressTrackerFixture
    with WorkerServiceFixture
    with Messaging
    with ScalaFutures {

  def withProgress[R](progressTracker: ProgressTracker)(
    testWith: TestWith[Progress, R]): R = {
    val createdProgress = createProgress

    whenReady(progressTracker.initialise(createdProgress)) { storedProgress =>
      testWith(storedProgress)
    }
  }

  def withProgressUpdateFlow[R](table: Table)(
    testWith: TestWith[(
                         Flow[ProgressUpdate, Progress, NotUsed],
                         ProgressTracker
                       ),
                       R]): R =
    withProgressTracker(table) { progressTracker =>
      testWith((ProgressUpdateFlow(progressTracker), progressTracker))
    }


  def withConfiguredApp[R](
    testWith: TestWith[(Queue, Topic, Table), R]): R = {
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
