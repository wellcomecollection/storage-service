package uk.ac.wellcome.platform.archive.ingests.services

import com.amazonaws.services.sns.model.AmazonSNSException
import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{
  Completed,
  Processing
}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IdConstraintError
import uk.ac.wellcome.platform.archive.ingests.fixtures.{
  IngestsFixture,
  WorkerServiceFixture
}

class IngestsWorkerServiceTest
    extends FunSpec
    with IngestGenerators
    with IngestsFixture
    with WorkerServiceFixture {
  it("updates an existing ingest to Completed") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            withIngestTracker(table) { monitor =>
              withIngest(monitor) { ingest =>
                val ingestStatusUpdate =
                  createIngestStatusUpdateWith(
                    id = ingest.id,
                    status = Completed
                  )

                val future = service.processMessage(ingestStatusUpdate)

                val expectedIngest = ingest.copy(
                  status = Completed,
                  events = ingestStatusUpdate.events,
                  bag = ingestStatusUpdate.affectedBag
                )

                val callbackNotification = CallbackNotification(
                  id = ingest.id,
                  callbackUri = ingest.callback.get.uri,
                  payload = expectedIngest
                )

                whenReady(future) { _ =>
                  assertSnsReceivesOnly(callbackNotification, topic = topic)

                  assertIngestCreated(expectedIngest, table)

                  assertIngestRecordedRecentEvents(
                    id = ingestStatusUpdate.id,
                    expectedEventDescriptions = ingestStatusUpdate.events.map {
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

  it("adds multiple events to an ingest") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            withIngestTracker(table) { monitor =>
              withIngest(monitor) { ingest =>
                val ingestStatusUpdate1 =
                  createIngestStatusUpdateWith(
                    id = ingest.id,
                    status = Processing
                  )

                val ingestStatusUpdate2 =
                  createIngestStatusUpdateWith(
                    id = ingest.id,
                    status = Processing,
                    maybeBag = ingestStatusUpdate1.affectedBag
                  )

                val expectedIngest = ingest.copy(
                  status = Completed,
                  events = ingestStatusUpdate1.events ++ ingestStatusUpdate2.events,
                  bag = ingestStatusUpdate1.affectedBag
                )

                val future1 = service.processMessage(ingestStatusUpdate1)
                whenReady(future1) { _ =>
                  val future2 = service.processMessage(ingestStatusUpdate2)
                  whenReady(future2) { _ =>
                    assertIngestCreated(expectedIngest, table)

                    val expectedEventDescriptions =
                      (ingestStatusUpdate1.events ++ ingestStatusUpdate2.events)
                        .map { _.description }

                    assertIngestRecordedRecentEvents(
                      id = ingestStatusUpdate1.id,
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

  it("fails if the ingest is not in the table") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        withLocalSnsTopic { topic =>
          withWorkerService(queue, table, topic) { service =>
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                status = Completed
              )

            val future = service.processMessage(ingestStatusUpdate)

            whenReady(future.failed) { err =>
              err shouldBe a[IdConstraintError]
            }
          }
        }

      }
    }
  }

  it("fails if publishing to SNS fails") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        withWorkerService(queue, table, Topic("does-not-exist")) { service =>
          withIngestTracker(table) { monitor =>
            withIngest(monitor) { ingest =>
              val ingestStatusUpdate =
                createIngestStatusUpdateWith(
                  id = ingest.id,
                  status = Completed
                )

              val future = service.processMessage(ingestStatusUpdate)

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
