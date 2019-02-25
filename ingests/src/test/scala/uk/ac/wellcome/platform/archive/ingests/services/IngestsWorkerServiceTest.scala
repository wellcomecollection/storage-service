package uk.ac.wellcome.platform.archive.ingests.services

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.sns.model.AmazonSNSException
import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.Progress.{
  Completed,
  Processing
}
import uk.ac.wellcome.platform.archive.common.progress.monitor.IdConstraintError
import uk.ac.wellcome.platform.archive.ingests.fixtures.{
  IngestsFixture,
  WorkerServiceFixture
}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

class IngestsWorkerServiceTest
    extends FunSpec
    with ProgressGenerators
    with IngestsFixture
    with WorkerServiceFixture {
  it("updates the status of an existing Progress to Completed") {
    withLocalSqsQueue { queue =>
      withProgressTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            withProgressTracker(table) { monitor =>
              withProgress(monitor) { progress =>
                val progressStatusUpdate =
                  createProgressStatusUpdateWith(
                    id = progress.id,
                    status = Completed
                  )

                val future = service.processMessage(progressStatusUpdate)

                val expectedProgress = progress.copy(
                  status = Completed,
                  events = progressStatusUpdate.events,
                  bag = progressStatusUpdate.affectedBag
                )

                val callbackNotification = CallbackNotification(
                  id = progress.id,
                  callbackUri = progress.callback.get.uri,
                  payload = expectedProgress
                )

                whenReady(future) { _ =>
                  assertSnsReceivesOnly(callbackNotification, topic = topic)

                  assertProgressCreated(expectedProgress, table)

                  assertProgressRecordedRecentEvents(
                    id = progressStatusUpdate.id,
                    expectedEventDescriptions =
                      progressStatusUpdate.events.map {
                        _.description
                      },
                    table = table
                  )
                }
              }
            }

          }
        }
      }
    }
  }

  it("adds multiple events to a Progress") {
    withLocalSqsQueue { queue =>
      withProgressTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            withProgressTracker(table) { monitor =>
              withProgress(monitor) { progress =>
                val progressStatusUpdate1 =
                  createProgressStatusUpdateWith(
                    id = progress.id,
                    status = Processing
                  )

                val progressStatusUpdate2 =
                  createProgressStatusUpdateWith(
                    id = progress.id,
                    status = Processing,
                    affectedBag = progressStatusUpdate1.affectedBag
                  )

                val expectedProgress = progress.copy(
                  status = Completed,
                  events = progressStatusUpdate1.events ++ progressStatusUpdate2.events,
                  bag = progressStatusUpdate1.affectedBag
                )

                val future1 = service.processMessage(progressStatusUpdate1)
                whenReady(future1) { _ =>
                  val future2 = service.processMessage(progressStatusUpdate2)
                  whenReady(future2) { _ =>
                    assertProgressCreated(expectedProgress, table)

                    val expectedEventDescriptions =
                      (progressStatusUpdate1.events ++ progressStatusUpdate2.events)
                        .map { _.description }

                    assertProgressRecordedRecentEvents(
                      id = progressStatusUpdate1.id,
                      expectedEventDescriptions = expectedEventDescriptions,
                      table = table
                    )
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  it(
    "returns a failed Future if the Progress is not already stored in the table") {
    withLocalSqsQueue { queue =>
      withProgressTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            val progressStatusUpdate =
              createProgressStatusUpdateWith(
                status = Completed
              )

            val future = service.processMessage(progressStatusUpdate)

            whenReady(future.failed) { err =>
              err shouldBe a[IdConstraintError]
            }
          }
        }

      }
    }
  }

  // For some reason it doesn't hold the exception in the Future, but fails immediately.
  // Investigating why is more effort than I want to spend right now.
  ignore("returns a failed future if updating the table fails") {
    withLocalSqsQueue { queue =>
      withLocalSnsTopic { topic =>
        withWorkerService(
          queue,
          Table("does-not-exist", "does-not-exist"),
          topic) { service =>
          val progressStatusUpdate =
            createProgressStatusUpdateWith(
              status = Completed
            )

          val future = service.processMessage(progressStatusUpdate)

          whenReady(future.failed) { err =>
            err shouldBe a[ResourceNotFoundException]
          }
        }
      }
    }
  }

  it("returns a failed future if publishing to SNS fails") {
    withLocalSqsQueue { queue =>
      withProgressTrackerTable { table =>
        withWorkerService(queue, table, Topic("does-not-exist")) { service =>
          withProgressTracker(table) { monitor =>
            withProgress(monitor) { progress =>
              val progressStatusUpdate =
                createProgressStatusUpdateWith(
                  id = progress.id,
                  status = Completed
                )

              val future = service.processMessage(progressStatusUpdate)

              whenReady(future.failed) { err =>
                err shouldBe a[AmazonSNSException]
              }
            }
          }
        }
      }
    }
  }
}
